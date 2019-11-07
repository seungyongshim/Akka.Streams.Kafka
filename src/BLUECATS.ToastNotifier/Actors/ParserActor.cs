using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
            Receive<string>(msg =>
            {
                
                dynamic json = JsonConvert.DeserializeObject(msg, new JsonSerializerSettings()
                {
                });
                var levelstring = json.Level.ToString();


            });
        }
    }
}
