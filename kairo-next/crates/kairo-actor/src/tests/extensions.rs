use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use crate::{ActorError, ActorSystem};

#[derive(Debug)]
struct CountingExtension {
    system_name: String,
}

#[derive(Debug)]
struct OtherExtension;

#[derive(Debug)]
struct ConcurrentExtension;

#[test]
fn extensions_are_created_once_and_retrieved_type_safely() {
    let system = ActorSystem::builder("extension-system").build().unwrap();
    let created = Arc::new(AtomicUsize::new(0));

    let first = system.register_extension({
        let created = Arc::clone(&created);
        move |system| {
            created.fetch_add(1, Ordering::SeqCst);
            CountingExtension {
                system_name: system.name().to_string(),
            }
        }
    });
    let second = system.register_extension({
        let created = Arc::clone(&created);
        move |_| {
            created.fetch_add(1, Ordering::SeqCst);
            CountingExtension {
                system_name: "should-not-be-created".to_string(),
            }
        }
    });
    let looked_up = system.extension::<CountingExtension>().unwrap();

    assert_eq!(created.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &looked_up));
    assert_eq!(looked_up.system_name, "extension-system");
    assert!(system.has_extension::<CountingExtension>());
    assert!(!system.has_extension::<OtherExtension>());
}

#[test]
fn missing_extension_lookup_reports_type_name() {
    let system = ActorSystem::builder("missing-extension").build().unwrap();

    let error = system.extension::<OtherExtension>().unwrap_err();

    assert!(
        matches!(error, ActorError::ExtensionNotRegistered(name) if name.ends_with("OtherExtension"))
    );
}

#[test]
fn extension_instances_are_scoped_to_one_actor_system() {
    let first_system = ActorSystem::builder("first-extension-system")
        .build()
        .unwrap();
    let second_system = ActorSystem::builder("second-extension-system")
        .build()
        .unwrap();

    let first = first_system.register_extension(|system| CountingExtension {
        system_name: system.name().to_string(),
    });
    let second = second_system.register_extension(|system| CountingExtension {
        system_name: system.name().to_string(),
    });

    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(first.system_name, "first-extension-system");
    assert_eq!(second.system_name, "second-extension-system");
}

#[test]
fn concurrent_extension_registration_creates_one_instance() {
    const THREADS: usize = 8;

    let system = Arc::new(
        ActorSystem::builder("concurrent-extension")
            .build()
            .unwrap(),
    );
    let created = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let system = Arc::clone(&system);
        let created = Arc::clone(&created);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            system.register_extension(move |_| {
                created.fetch_add(1, Ordering::SeqCst);
                ConcurrentExtension
            })
        }));
    }

    let mut extensions = Vec::new();
    for handle in handles {
        extensions.push(handle.join().unwrap());
    }

    assert_eq!(created.load(Ordering::SeqCst), 1);
    for extension in &extensions[1..] {
        assert!(Arc::ptr_eq(&extensions[0], extension));
    }
}
