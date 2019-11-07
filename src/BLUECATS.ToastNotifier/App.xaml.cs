using System;
using System.Reflection;
using System.Threading;
using System.Windows;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using BLUECATS.ToastNotifier.Actors;
using Confluent.Kafka;
using ToastNotifications;
using ToastNotifications.Lifetime;
using ToastNotifications.Lifetime.Clear;
using ToastNotifications.Messages;
using ToastNotifications.Position;
using System.Linq;
using System.Text;
using Akka.Streams.Dsl;

namespace BLUECATS.ToastNotifier
{
    /// <summary>
    /// App.xaml에 대한 상호 작용 논리
    /// </summary>
    public partial class App : Application
    {
        private IActorRef notificationActor;

        public System.Windows.Forms.NotifyIcon NotifyIcon { get; set; }
        public Notifier Notifier { get; set; }
        public string GUID { get; set; }

        protected override void OnStartup(StartupEventArgs e)
        {
            GUID = GetGUID();

            var mtx = new Mutex(true, GUID, out bool createdNew);

            // 뮤텍스를 얻지 못하면 에러
            if (!createdNew)
            {
                MessageBox.Show("이미 실행중입니다.");
                Shutdown();
                return;
            }

            try
            {
                var config = AkkaHelper.ReadConfigurationFromHoconFile(Assembly.GetExecutingAssembly(), "conf")
                    .WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

                CreateTrayIcon(config);
                CreateNotifier(config);

                var system = ActorSystem.Create("BLUECATS-ToastNotifier", config);

                notificationActor = system.ActorOf(NotificationActor.Props(Notifier), nameof(NotificationActor));
                var parserActor = system.ActorOf(ParserActor.Props(notificationActor), nameof(ParserActor));

                var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, Deserializers.Utf8)
                    .WithBootstrapServers(GetBootStrapServers(config))
                    .WithGroupId(GUID);

                notificationActor.Tell((NotificationLevel.Info, $"BLUE CATS: Client Start\n{GUID}"));

                RestartSource.WithBackoff(() =>
                    KafkaConsumer.PlainSource(consumerSettings, GetSubscription(config)),
                    minBackoff: TimeSpan.FromSeconds(3),
                    maxBackoff: TimeSpan.FromSeconds(30),
                    randomFactor: 0.2).RunForeach(result =>
                    {
                        parserActor.Tell(result);
                    }, system.Materializer());
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }

            base.OnStartup(e);
        }

        private static string GetBootStrapServers(Akka.Configuration.Config config)
        {
            return config.GetStringList("kafka.bootstrap-servers")
                .Aggregate(new StringBuilder(), (builder, item) => builder.Append(builder.Length == 0 ? "" : ", ").Append(item))
                .ToString();
        }

        private IAutoSubscription GetSubscription(Akka.Configuration.Config config)
        {
            var topicpattern = config.GetString("kafka.topic-pattern");
            var topics = config.GetString("kafka.topics");

            return topicpattern != null ? Subscriptions.TopicPattern(topicpattern) : Subscriptions.Topics(topics);
        }

        private string GetGUID()
        {
            Microsoft.Win32.RegistryKey mykey;
            mykey = Microsoft.Win32.Registry.CurrentUser.CreateSubKey("BLUECATS");
            var guid = mykey.GetValue("GUID", Guid.NewGuid());
            mykey.SetValue("GUID", guid);
            mykey.Close();

            return guid.ToString();
        }

        private void CreateNotifier(Akka.Configuration.Config config)
        {
            var uiNotificationWidth = config.GetInt("ui.notification.width");
            var uiNotificationLifetime = config.GetTimeSpan("ui.notification.lifetime");
            var uiNotificationMax = config.GetInt("ui.notification.maximum-count");

            Notifier = new Notifier(cfg =>
            {
                cfg.DisplayOptions.Width = uiNotificationWidth;

                cfg.PositionProvider = new PrimaryScreenPositionProvider(Corner.BottomRight, 0, 0);

                cfg.LifetimeSupervisor = new TimeAndCountBasedLifetimeSupervisor(
                    notificationLifetime: uiNotificationLifetime,
                    maximumNotificationCount: MaximumNotificationCount.FromCount(uiNotificationMax));

                cfg.Dispatcher = Application.Current.Dispatcher;
            });
        }

        private void CreateTrayIcon(Akka.Configuration.Config config)
        {
            NotifyIcon = new System.Windows.Forms.NotifyIcon
            {
                Icon = ToastNotifier.Properties.Resources.favicon,
                Text = "BLUE CATS Client"
            };
            var menu = new System.Windows.Forms.ContextMenu();
            NotifyIcon.ContextMenu = menu;
            NotifyIcon.Visible = true;

            menu.MenuItems.Add(new System.Windows.Forms.MenuItem(@"&All Clear",
                onClick: (_, __) =>
                {
                    //notificationActor.Tell(new ClearAll());
                    Notifier.ClearMessages(new ClearAll());
                }));

            menu.MenuItems.Add(new System.Windows.Forms.MenuItem(@"&Exit",
                    onClick: (_, __) =>
                    {
                        NotifyIcon.Visible = false;
                        Shutdown();
                    }));

            
        }
    }
}
