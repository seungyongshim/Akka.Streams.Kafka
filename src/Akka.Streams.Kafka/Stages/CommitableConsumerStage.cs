﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;
using Akka.Streams.Supervision;
using System.Runtime.Serialization;
using System.Threading;

namespace Akka.Streams.Kafka.Stages
{
    internal class CommitableConsumerStage<K, V, Msg> : GraphStageWithMaterializedValue<SourceShape<Msg>, CancellationTokenSource>
    {
        public Outlet<Msg> Out { get; } = new Outlet<Msg>("kafka.commitable.consumer.out");
        public override SourceShape<Msg> Shape { get; }
        public ConsumerSettings<K, V> Settings { get; }
        public ISubscription Subscription { get; }

        public CommitableConsumerStage(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            Settings = settings;
            Subscription = subscription;
            Shape = new SourceShape<Msg>(Out);
        }

        public override ILogicAndMaterializedValue<CancellationTokenSource> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var token = cancellationTokenSource.Token;
            return new LogicAndMaterializedValue<CancellationTokenSource>(new KafkaCommitableSourceStage<K, V, Msg>(this, inheritedAttributes, token), cancellationTokenSource);
        }
    }

    internal class KafkaCommitableSourceStage<K, V, Msg> : TimerGraphStageLogic
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;
        private readonly Outlet _out;
        private Consumer<K, V> _consumer;

        private Action<Message<K, V>> _messagesReceived;
        private Action<IEnumerable<TopicPartition>> _partitionsAssigned;
        private Action<IEnumerable<TopicPartition>> _partitionsRevoked;
        private readonly Decider _decider;

        private const string TimerKey = "PollTimer";

        private readonly Queue<CommittableMessage<K, V>> _buffer;
        private IEnumerable<TopicPartition> assignedPartitions = null;
        private volatile bool isPaused = false;
        private readonly CancellationToken _token;

        public KafkaCommitableSourceStage(CommitableConsumerStage<K, V, Msg> stage, Attributes attributes, CancellationToken token) : base(stage.Shape)
        {
            _settings = stage.Settings;
            _subscription = stage.Subscription;
            _out = stage.Out;
            _token = token;
            _buffer = new Queue<CommittableMessage<K, V>>(stage.Settings.BufferSize);

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.ResumingDecider;

            SetHandler(_out, onPull: () =>
            {
                if (_buffer.Count > 0)
                {
                    Push(_out, _buffer.Dequeue());
                }
                else
                {
                    if (isPaused)
                    {
                        _consumer.Resume(assignedPartitions);
                        isPaused = false;
                        Log.Debug($"Polling resumed, buffer is empty");
                    }
                    PullQueue();
                }
            });
        }

        public override void PreStart()
        {
            base.PreStart();

            _consumer = _settings.CreateKafkaConsumer();
            Log.Debug($"Consumer started: {_consumer.Name}");

            _consumer.OnMessage += HandleOnMessage;
            _consumer.OnConsumeError += HandleConsumeError;
            _consumer.OnError += HandleOnError;
            _consumer.OnPartitionsAssigned += HandleOnPartitionsAssigned;
            _consumer.OnPartitionsRevoked += HandleOnPartitionsRevoked;

            switch (_subscription)
            {
                case TopicSubscription ts:
                    _consumer.Subscribe(ts.Topics);
                    break;
                case Assignment a:
                    _consumer.Assign(a.TopicPartitions);
                    break;
                case AssignmentWithOffset awo:
                    _consumer.Assign(awo.TopicPartitions);
                    break;
            }

            _messagesReceived = GetAsyncCallback<Message<K, V>>(MessagesReceived);
            _partitionsAssigned = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsAssigned);
            _partitionsRevoked = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsRevoked);
            ScheduleRepeatedly(TimerKey, _settings.PollInterval);
        }

        public override void PostStop()
        {
            _consumer.OnMessage -= HandleOnMessage;
            _consumer.OnConsumeError -= HandleConsumeError;
            _consumer.OnError -= HandleOnError;
            _consumer.OnPartitionsAssigned -= HandleOnPartitionsAssigned;
            _consumer.OnPartitionsRevoked -= HandleOnPartitionsRevoked;

            Log.Debug($"Consumer stopped: {_consumer.Name}");
            _consumer.Dispose();

            base.PostStop();
        }

        //
        // Consumer's events
        //

        private void HandleOnMessage(object sender, Message<K, V> message) => _messagesReceived.Invoke(message);

        private void HandleConsumeError(object sender, Message message)
        {
            Log.Error(message.Error.Reason);
            var exception = new SerializationException(message.Error.Reason);
            switch (_decider(exception))
            {
                case Directive.Stop:
                    // Throw
                    FailStage(exception);
                    break;
                case Directive.Resume:
                    // keep going
                    break;
                case Directive.Restart:
                    // keep going
                    break;
            }
        }

        private void HandleOnError(object sender, Error error)
        {
            Log.Error(error.Reason);

            if (!KafkaExtensions.IsBrokerErrorRetriable(error) && !KafkaExtensions.IsLocalErrorRetriable(error))
            {
                var exception = new KafkaException(error);
                FailStage(exception);
            }
        }

        private void HandleOnPartitionsAssigned(object sender, List<TopicPartition> list)
        {
            _partitionsAssigned.Invoke(list);
        }

        private void HandleOnPartitionsRevoked(object sender, List<TopicPartition> list)
        {
            _partitionsRevoked.Invoke(list);
        }

        //
        // Async callbacks
        //

        private void MessagesReceived(Message<K, V> message)
        {
            var consumer = _consumer;
            var commitableOffset = new CommitableOffset(
                () => consumer.CommitAsync(),
                new PartitionOffset("groupId", message.Topic, message.Partition, message.Offset));

            _buffer.Enqueue(new CommittableMessage<K, V>(message, commitableOffset));
            if (IsAvailable(_out))
            {
                Push(_out, _buffer.Dequeue());
            }
        }

        private void PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Log.Debug($"Partitions were assigned: {_consumer.Name}");
            _consumer.Assign(partitions);
            assignedPartitions = partitions;
        }

        private void PartitionsRevoked(IEnumerable<TopicPartition> partitions)
        {
            Log.Debug($"Partitions were revoked: {_consumer.Name}");
            _consumer.Unassign();
            assignedPartitions = null;
        }

        private void PullQueue()
        {
            if (_token.IsCancellationRequested)
            {
                CompleteStage();
            }

            _consumer.Poll(_settings.PollTimeout);

            if (!isPaused && _buffer.Count > _settings.BufferSize)
            {
                Log.Debug($"Polling paused, buffer is full");
                _consumer.Pause(assignedPartitions);
                isPaused = true;
            }
        }

        protected override void OnTimer(object timerKey) => PullQueue();
    }
}
