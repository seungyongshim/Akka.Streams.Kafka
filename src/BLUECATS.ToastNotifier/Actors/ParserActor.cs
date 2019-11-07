using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using ToastNotifications;
using ToastNotifications.Messages;

namespace BLUECATS.ToastNotifier.Actors
{
    public class ParserActor : ReceiveActor
    {
        public static Props Props(IActorRef notificationActor)
        {
            return Akka.Actor.Props.Create(() => new ParserActor(notificationActor));
        }

        public ParserActor(IActorRef notificationActor)
        {
            Receive<ConsumeResult<Null, string>>(msg =>
            {
                dynamic json = JsonConvert.DeserializeObject(msg.Value, new JsonSerializerSettings()
                {
                    DateTimeZoneHandling = DateTimeZoneHandling.Local,
                });
                string message = json.message.ToString();
                string localtime = json["@timestamp"].ToString("yyyy-MM-dd HH:mm:ss.ffffff");


                var sb = new StringBuilder();
                notificationActor.Tell(
                    (NotificationLevel.Error,
                    sb.AppendLine(string.Format($"[{msg.Topic}] {localtime}")).Append(message).ToString())
                );
            });

        }
    }
}
