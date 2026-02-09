//! LLM-related commands.

use console::style;

use foiacquire::config::{Config, Settings};
use foiacquire::llm::LlmClient;

/// List available LLM models.
pub async fn cmd_llm_models(_settings: &Settings) -> anyhow::Result<()> {
    let config = Config::load().await;
    let llm_client = LlmClient::new(config.llm.clone());

    println!("\n{}", style("LLM Configuration").bold());
    println!("{}", "-".repeat(40));
    println!(
        "{:<20} {}",
        "Enabled:",
        if config.llm.enabled() { "Yes" } else { "No" }
    );
    println!("{:<20} {}", "Provider:", config.llm.provider_name());
    println!("{:<20} {}", "Endpoint:", config.llm.endpoint());
    println!(
        "{:<20} {}",
        "API Key:",
        if config.llm.api_key().is_some() {
            "Set"
        } else {
            "Not set"
        }
    );
    println!("{:<20} {}", "Current Model:", config.llm.model());
    println!("{:<20} {}", "Max Tokens:", config.llm.max_tokens());
    println!("{:<20} {:.2}", "Temperature:", config.llm.temperature());

    if !llm_client.is_available().await {
        println!(
            "\n{} {}",
            style("!").yellow(),
            config.llm.availability_hint()
        );
        return Ok(());
    }

    println!("\n{}", style("Available Models").bold());
    println!("{}", "-".repeat(40));

    match llm_client.list_models().await {
        Ok(models) => {
            if models.is_empty() {
                println!("  No models available");
            } else {
                for model in models {
                    let marker = if model == config.llm.model() {
                        style("*").green().to_string()
                    } else {
                        " ".to_string()
                    };
                    println!("{} {}", marker, model);
                }
            }
        }
        Err(e) => {
            println!("{} Failed to list models: {}", style("✗").red(), e);
        }
    }

    Ok(())
}
