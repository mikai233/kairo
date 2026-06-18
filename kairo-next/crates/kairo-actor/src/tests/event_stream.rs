use super::*;

#[derive(Clone)]
enum EventStreamProbeMsg {
    Event(&'static str),
    Subscribe(mpsc::Sender<bool>),
    Unsubscribe(mpsc::Sender<bool>),
}

#[derive(Clone)]
struct OtherEvent;

struct EventStreamProbe {
    observed: mpsc::Sender<&'static str>,
}

struct DeadLetterSubscriber {
    stopped: mpsc::Sender<()>,
}

impl Actor for EventStreamProbe {
    type Msg = EventStreamProbeMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            EventStreamProbeMsg::Event(label) => {
                self.observed
                    .send(label)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Subscribe(reply_to) => {
                let subscribed = ctx.event_stream().subscribe(ctx.myself());
                reply_to
                    .send(subscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            EventStreamProbeMsg::Unsubscribe(reply_to) => {
                let unsubscribed = ctx.event_stream().unsubscribe(&ctx.myself());
                reply_to
                    .send(unsubscribed)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

impl Actor for DeadLetterSubscriber {
    type Msg = DeadLetter;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.stopped
            .send(())
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn event_stream_publishes_to_typed_subscribers_once() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(first_tx))
        .unwrap();
    actor
        .tell(EventStreamProbeMsg::Subscribe(second_tx))
        .unwrap();

    assert!(first_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    assert!(!second_rx.recv_timeout(Duration::from_secs(1)).unwrap());

    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "event"
    );
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_unsubscribe_stops_delivery() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();
    let (unsubscribe_tx, unsubscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    actor
        .tell(EventStreamProbeMsg::Unsubscribe(unsubscribe_tx))
        .unwrap();

    assert!(unsubscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    system
        .event_stream()
        .publish(EventStreamProbeMsg::Event("event"));

    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_matches_exact_event_type() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let actor = system
        .spawn(
            "events",
            Props::new(move || EventStreamProbe {
                observed: observed_tx,
            }),
        )
        .unwrap();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();

    actor
        .tell(EventStreamProbeMsg::Subscribe(subscribe_tx))
        .unwrap();
    subscribe_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.event_stream().publish(OtherEvent);
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn dead_letters_are_published_to_event_stream() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel::<DeadLetter>();
    let subscriber = system
        .spawn(
            "dead-letter-subscriber",
            Props::new(move || ChannelProbe {
                observed: dead_letter_tx,
            }),
        )
        .unwrap();
    assert!(system.event_stream().subscribe(subscriber));
    let missing: ActorRef<CounterMsg> = system.missing_ref("kairo://test/user/missing#404");

    let error = missing.tell(CounterMsg::Increment).unwrap_err();

    assert_eq!(error.reason(), "actor does not exist");
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let event = dead_letter_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event.recipient(), missing.path());
    assert_eq!(event.reason(), "actor does not exist");
    assert_eq!(event.message_type(), std::any::type_name::<CounterMsg>());
}

#[test]
fn dead_letter_event_stream_publication_can_be_disabled() {
    let system = ActorSystem::builder("test")
        .publish_dead_letters_to_event_stream(false)
        .build()
        .unwrap();
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel::<DeadLetter>();
    let subscriber = system
        .spawn(
            "dead-letter-subscriber",
            Props::new(move || ChannelProbe {
                observed: dead_letter_tx,
            }),
        )
        .unwrap();
    assert!(system.event_stream().subscribe(subscriber));
    let missing: ActorRef<CounterMsg> = system.missing_ref("kairo://test/user/missing#404");

    missing.tell(CounterMsg::Increment).unwrap_err();

    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), missing.path());
    assert_eq!(records[0].reason(), "actor does not exist");
    assert_eq!(
        records[0].message_type(),
        std::any::type_name::<CounterMsg>()
    );
    assert!(
        dead_letter_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
}

#[test]
fn event_stream_prunes_failed_dead_letter_subscribers() {
    let system = ActorSystem::builder("test").build().unwrap();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let subscriber = system
        .spawn(
            "dead-letter-subscriber",
            Props::new(move || DeadLetterSubscriber {
                stopped: stopped_tx,
            }),
        )
        .unwrap();
    assert!(system.event_stream().subscribe(subscriber.clone()));
    system.stop(&subscriber);
    assert!(subscriber.wait_for_stop(Duration::from_secs(1)));
    stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let missing: ActorRef<CounterMsg> = system.missing_ref("kairo://test/user/missing#404");
    missing.tell(CounterMsg::Increment).unwrap_err();

    assert!(
        system
            .dead_letters()
            .wait_for_len(2, Duration::from_secs(1))
    );
    let records = system.dead_letters().records();
    assert_eq!(records[0].recipient(), missing.path());
    assert_eq!(
        records[0].message_type(),
        std::any::type_name::<CounterMsg>()
    );
    assert_eq!(records[1].recipient(), subscriber.path());
    assert_eq!(
        records[1].message_type(),
        std::any::type_name::<DeadLetter>()
    );

    missing.tell(CounterMsg::Increment).unwrap_err();
    assert!(
        system
            .dead_letters()
            .wait_for_len(3, Duration::from_secs(1))
    );
    assert_eq!(system.dead_letters().len(), 3);
}
