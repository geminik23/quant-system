//! Color and spacing constants for the static shell.

use gpui::{Rgba, rgb};

pub const TEXT_SIZE: f32 = 13.0;
pub const TITLE_SIZE: f32 = 15.0;
pub const SMALL_SIZE: f32 = 11.0;

pub const HEADER_HEIGHT: f32 = 40.0;
pub const PHASE_BAR_HEIGHT: f32 = 34.0;
pub const STATUS_BAR_HEIGHT: f32 = 26.0;
pub const CONTEXT_NAV_WIDTH: f32 = 220.0;
pub const INSPECTOR_WIDTH: f32 = 330.0;

pub const PHASE_MIN_WIDTH: f32 = 1180.0;
pub const PHASE_MIN_HEIGHT: f32 = 720.0;
pub const PHASE_DEFAULT_WIDTH: f32 = 1440.0;
pub const PHASE_DEFAULT_HEIGHT: f32 = 900.0;

pub fn window_bg() -> Rgba {
    rgb(0xF4F5F7)
}

pub fn panel_bg() -> Rgba {
    rgb(0xFFFFFF)
}

pub fn chip_bg() -> Rgba {
    rgb(0xE9EDF2)
}

pub fn hover_bg() -> Rgba {
    rgb(0xEAF0FB)
}

pub fn border() -> Rgba {
    rgb(0xD5DAE0)
}

pub fn text() -> Rgba {
    rgb(0x1F2328)
}

pub fn dim_text() -> Rgba {
    rgb(0x5A6472)
}

pub fn accent() -> Rgba {
    rgb(0x2F6FED)
}

pub fn ok_green() -> Rgba {
    rgb(0x1A7F37)
}

pub fn warn_amber() -> Rgba {
    rgb(0x9A6700)
}

pub fn error_red() -> Rgba {
    rgb(0xCF222E)
}

pub fn offline_purple() -> Rgba {
    rgb(0x8250DF)
}

pub fn selection_track() -> Rgba {
    rgb(0xE4E8EE)
}
