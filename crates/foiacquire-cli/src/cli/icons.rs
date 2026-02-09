//! Console output icons and styled text helpers.
//!
//! Provides standardized icons for CLI output to ensure consistent styling
//! across all commands.

#![allow(dead_code)]

use console::{style, StyledObject};

/// Success checkmark icon (green ✓).
pub fn success() -> StyledObject<&'static str> {
    style("✓").green()
}

/// Info/progress arrow icon (cyan →).
pub fn info() -> StyledObject<&'static str> {
    style("→").cyan()
}

/// Warning icon (yellow !).
pub fn warn() -> StyledObject<&'static str> {
    style("!").yellow()
}

/// Error icon (red ✗).
pub fn error() -> StyledObject<&'static str> {
    style("✗").red()
}

/// Dim arrow for secondary info.
pub fn dim_arrow() -> StyledObject<&'static str> {
    style("→").dim()
}

/// Bullet point.
pub fn bullet() -> StyledObject<&'static str> {
    style("•").dim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_icons_dont_panic() {
        // Just ensure they return something
        let _ = success().to_string();
        let _ = info().to_string();
        let _ = warn().to_string();
        let _ = error().to_string();
        let _ = dim_arrow().to_string();
        let _ = bullet().to_string();
    }
}
