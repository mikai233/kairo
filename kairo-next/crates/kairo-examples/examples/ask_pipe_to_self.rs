use kairo_examples::patterns::{PatternObservation, run_ask_pipe_to_self};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let observations = run_ask_pipe_to_self("ask-pipe-to-self", 21)?;

    for observation in observations {
        match observation {
            PatternObservation::AskCompleted { input, output } => {
                println!("ask completed: {input} -> {output}");
            }
            PatternObservation::AskFailed { reason } => {
                println!("ask failed: {reason}");
            }
            PatternObservation::PipeCompleted { input, output } => {
                println!("pipe_to_self completed: {input} -> {output}");
            }
            PatternObservation::PipeFailed { input, reason } => {
                println!("pipe_to_self failed for {input}: {reason}");
            }
        }
    }

    Ok(())
}
