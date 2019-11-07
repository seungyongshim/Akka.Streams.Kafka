using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ToastNotifications;
using ToastNotifications.Lifetime.Clear;
using ToastNotifications.Messages;

namespace BLUECATS.ToastNotifier.Actors
{
    public class NotificationActor : ReceiveActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        public static Props Props(Notifier notifier)
        {
            return Akka.Actor.Props.Create(() => new NotificationActor(notifier));
        }

        public NotificationActor(Notifier notifier)
        {
            Receive<(NotificationLevel,string)>( _ =>
            {
                var (lv, msg) = _;

                switch (lv)
                {
                    case NotificationLevel.Info:
                        notifier.ShowInformation(msg);
                        break;
                    case NotificationLevel.Warning:
                        notifier.ShowWarning(msg);
                        break;
                    case NotificationLevel.Error:
                        notifier.ShowError(msg);
                        break;
                }
                
                BecomeStacked(Delaying);
            });
        }

        private void Delaying()
        {
            Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.5), Self, DelayMessage.Instance, Self);
            Receive<DelayMessage>(_ =>
            {
                Stash.Unstash();
                UnbecomeStacked();
            });

            ReceiveAny(_ => Stash.Stash());
        }

        internal class DelayMessage
        {
            public static DelayMessage Instance { get; } = new DelayMessage();
        }

        internal class ClearedMessage
        {
            public static ClearedMessage Instance { get; } = new ClearedMessage();
        }
    }
}
