use super::*;

struct ListingProbe {
    observed: mpsc::Sender<Vec<ActorPath>>,
}

impl Actor for ListingProbe {
    type Msg = Listing<()>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let paths = msg
            .service_instances()
            .iter()
            .map(|service| service.path().clone())
            .collect();
        self.observed
            .send(paths)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct RegisteredProbe {
    observed: mpsc::Sender<(String, ActorPath)>,
}

impl Actor for RegisteredProbe {
    type Msg = Registered<()>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((
                msg.key().id().to_string(),
                msg.service_instance().path().clone(),
            ))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

struct DeregisteredProbe {
    observed: mpsc::Sender<(String, ActorPath)>,
}

impl Actor for DeregisteredProbe {
    type Msg = Deregistered<()>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((
                msg.key().id().to_string(),
                msg.service_instance().path().clone(),
            ))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[test]
fn receptionist_subscribe_gets_initial_listing_and_updates() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<()>::new("svc");
    let (listing_tx, listing_rx) = mpsc::channel();
    let subscriber = system
        .spawn(
            "listing-probe",
            Props::new(move || ListingProbe {
                observed: listing_tx,
            }),
        )
        .unwrap();

    assert!(
        system
            .receptionist()
            .subscribe(key.clone(), subscriber.clone())
    );
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );

    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();
    assert!(system.receptionist().register(key.clone(), service.clone()));
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![service.path().clone()]
    );
    assert_eq!(
        system.receptionist().find(&key).service_instances()[0].path(),
        service.path()
    );

    assert!(system.receptionist().deregister(&key, &service));
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
}

#[test]
fn local_receptionist_listing_reports_all_services_as_reachable() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<()>::new("svc");
    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();

    assert!(system.receptionist().register(key.clone(), service.clone()));
    let listing = system.receptionist().find(&key);

    assert!(listing.services_were_added_or_removed());
    assert_eq!(
        listing
            .service_instances()
            .iter()
            .map(ActorRef::path)
            .collect::<Vec<_>>(),
        vec![service.path()]
    );
    assert_eq!(
        listing
            .all_service_instances()
            .iter()
            .map(ActorRef::path)
            .collect::<Vec<_>>(),
        vec![service.path()]
    );
}

#[test]
fn receptionist_register_with_ack_confirms_processed_registration() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<()>::new("svc");
    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();
    let (ack_tx, ack_rx) = mpsc::channel();
    let reply_to = system
        .spawn(
            "registered-probe",
            Props::new(move || RegisteredProbe { observed: ack_tx }),
        )
        .unwrap();

    assert!(
        system
            .receptionist()
            .register_with_ack(key.clone(), service.clone(), reply_to.clone())
            .unwrap()
    );
    assert_eq!(
        ack_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("svc".to_string(), service.path().clone())
    );

    assert!(
        !system
            .receptionist()
            .register_with_ack(key.clone(), service.clone(), reply_to)
            .unwrap()
    );
    assert_eq!(
        ack_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("svc".to_string(), service.path().clone())
    );
    assert_eq!(
        system.receptionist().find(&key).service_instances().len(),
        1
    );
}

#[test]
fn receptionist_deregister_with_ack_confirms_removed_registration() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<()>::new("svc");
    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();
    let (ack_tx, ack_rx) = mpsc::channel();
    let reply_to = system
        .spawn(
            "deregistered-probe",
            Props::new(move || DeregisteredProbe { observed: ack_tx }),
        )
        .unwrap();

    assert!(system.receptionist().register(key.clone(), service.clone()));
    assert!(
        system
            .receptionist()
            .deregister_with_ack(&key, &service, reply_to.clone())
            .unwrap()
    );
    assert_eq!(
        ack_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("svc".to_string(), service.path().clone())
    );
    assert!(
        system
            .receptionist()
            .find(&key)
            .service_instances()
            .is_empty()
    );

    assert!(
        !system
            .receptionist()
            .deregister_with_ack(&key, &service, reply_to)
            .unwrap()
    );
    assert!(ack_rx.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn receptionist_removes_registered_service_on_actor_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<()>::new("svc");
    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();
    let (listing_tx, listing_rx) = mpsc::channel();
    let subscriber = system
        .spawn(
            "listing-probe",
            Props::new(move || ListingProbe {
                observed: listing_tx,
            }),
        )
        .unwrap();

    assert!(system.receptionist().register(key.clone(), service.clone()));
    assert!(system.receptionist().subscribe(key, subscriber));
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![service.path().clone()]
    );

    system.stop(&service);
    assert!(service.wait_for_stop(Duration::from_secs(1)));

    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
}

#[test]
fn receptionist_removes_service_from_all_keys_on_actor_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key_a = ServiceKey::<()>::new("svc-a");
    let key_b = ServiceKey::<()>::new("svc-b");
    let service = system.spawn("svc", Props::new(|| Noop)).unwrap();
    let (listing_a_tx, listing_a_rx) = mpsc::channel();
    let subscriber_a = system
        .spawn(
            "listing-probe-a",
            Props::new(move || ListingProbe {
                observed: listing_a_tx,
            }),
        )
        .unwrap();
    let (listing_b_tx, listing_b_rx) = mpsc::channel();
    let subscriber_b = system
        .spawn(
            "listing-probe-b",
            Props::new(move || ListingProbe {
                observed: listing_b_tx,
            }),
        )
        .unwrap();

    assert!(system.receptionist().subscribe(key_a.clone(), subscriber_a));
    assert!(system.receptionist().subscribe(key_b.clone(), subscriber_b));
    assert_eq!(
        listing_a_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
    assert_eq!(
        listing_b_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );

    assert!(system.receptionist().register(key_a, service.clone()));
    assert!(system.receptionist().register(key_b, service.clone()));
    assert_eq!(
        listing_a_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![service.path().clone()]
    );
    assert_eq!(
        listing_b_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        vec![service.path().clone()]
    );

    system.stop(&service);
    assert!(service.wait_for_stop(Duration::from_secs(1)));

    assert_eq!(
        listing_a_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
    assert_eq!(
        listing_b_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
}

#[test]
fn receptionist_removes_subscriber_from_all_keys_on_actor_stop() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key_a = ServiceKey::<()>::new("svc-a");
    let key_b = ServiceKey::<()>::new("svc-b");
    let (listing_tx, listing_rx) = mpsc::channel();
    let subscriber = system
        .spawn(
            "listing-probe",
            Props::new(move || ListingProbe {
                observed: listing_tx,
            }),
        )
        .unwrap();

    assert!(
        system
            .receptionist()
            .subscribe(key_a.clone(), subscriber.clone())
    );
    assert!(
        system
            .receptionist()
            .subscribe(key_b.clone(), subscriber.clone())
    );
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );
    assert_eq!(
        listing_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Vec::<ActorPath>::new()
    );

    system.stop(&subscriber);
    assert!(subscriber.wait_for_stop(Duration::from_secs(1)));

    let service_a = system.spawn("svc-a", Props::new(|| Noop)).unwrap();
    let service_b = system.spawn("svc-b", Props::new(|| Noop)).unwrap();
    assert!(system.receptionist().register(key_a.clone(), service_a));
    assert!(system.receptionist().register(key_b.clone(), service_b));

    assert_eq!(system.dead_letters().len(), 0);
    assert_eq!(
        system.receptionist().find(&key_a).service_instances().len(),
        1
    );
    assert_eq!(
        system.receptionist().find(&key_b).service_instances().len(),
        1
    );
}

enum ContextReceptionistMsg {
    RegisterSelf {
        key: ServiceKey<ContextReceptionistMsg>,
        reply_to: mpsc::Sender<bool>,
    },
}

struct ContextReceptionistProbe;

impl Actor for ContextReceptionistProbe {
    type Msg = ContextReceptionistMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ContextReceptionistMsg::RegisterSelf { key, reply_to } => {
                let registered = ctx.receptionist().register(key, ctx.myself());
                reply_to
                    .send(registered)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[test]
fn context_exposes_receptionist_handle() {
    let system = ActorSystem::builder("test").build().unwrap();
    let key = ServiceKey::<ContextReceptionistMsg>::new("ctx-svc");
    let service = system
        .spawn("ctx-service", Props::new(|| ContextReceptionistProbe))
        .unwrap();
    let (reply_tx, reply_rx) = mpsc::channel();

    service
        .tell(ContextReceptionistMsg::RegisterSelf {
            key: key.clone(),
            reply_to: reply_tx,
        })
        .unwrap();

    assert!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    assert_eq!(
        system.receptionist().find(&key).service_instances()[0].path(),
        service.path()
    );
}
