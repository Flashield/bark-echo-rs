//! The CJK red line (ADR-0003): egui ships no CJK glyphs by default, and
//! the text this GUI shows is user-defined — Subscription labels may be
//! any language, and the Push Composer's input may be Chinese. The
//! system's CJK-capable font is therefore loaded explicitly at startup,
//! before any UI exists, and inserted as the fallback of both text
//! families: Latin keeps egui's default rendering, every glyph the
//! defaults lack resolves to the system font.
//!
//! eframe 0.37 additionally loads system fonts on demand per missing
//! glyph (`system_font_fallback`); this module stays the deterministic
//! foundation — the first frame renders user text from a font chosen
//! here, not from whatever the on-demand machinery discovers later.

/// The system CJK fonts worth trying, first hit wins, per platform.
/// Desktop installs always carry at least one (macOS PingFang, Windows
/// Microsoft YaHei, Linux Noto CJK or WenQuanYi); a stripped headless
/// container may carry none, and the GUI then runs on egui's defaults.
#[cfg(target_os = "macos")]
pub fn cjk_font_candidates() -> &'static [&'static str] {
    &[
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
}

/// Platform fact: Windows ships CJK fonts under `C:\Windows\Fonts` —
/// Microsoft YaHei (clearType UI font since Vista) is the modern pick,
/// SimHei/SimSun the legacy fallbacks.
#[cfg(windows)]
pub fn cjk_font_candidates() -> &'static [&'static str] {
    &[
        "C:\\Windows\\Fonts\\msyh.ttc",
        "C:\\Windows\\Fonts\\simhei.ttf",
        "C:\\Windows\\Fonts\\simsun.ttc",
    ]
}

/// Platform fact: Linux has no one CJK font path — distributions differ
/// in packaging (Debian's `fonts-noto-cjk` lands under
/// `opentype/noto`, Fedora's under `noto-cjk`; WenQuanYi and AR PL are
/// the common lightweight alternatives). Every plausible location of a
/// desktop install's CJK font is a candidate.
#[cfg(all(unix, not(target_os = "macos")))]
pub fn cjk_font_candidates() -> &'static [&'static str] {
    &[
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/wenquanyi/wqy-microhei/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/arphic/uming.ttc",
    ]
}

/// The first candidate font's bytes, `None` when the system has none of
/// them (the stripped-install case above).
pub fn load_cjk_font() -> Option<Vec<u8>> {
    cjk_font_candidates()
        .iter()
        .find_map(|path| std::fs::read(path).ok())
}

/// Insert `bytes` into egui's definitions as the LAST fallback of both
/// text families — a fallback is consulted only for glyphs the earlier
/// fonts lack, so Latin (and everything else the defaults cover)
/// renders exactly as egui always did.
pub fn insert_cjk_fallback(definitions: &mut egui::FontDefinitions, bytes: Vec<u8>, name: &str) {
    definitions.font_data.insert(
        name.to_owned(),
        std::sync::Arc::new(egui::FontData::from_owned(bytes)),
    );
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        definitions
            .families
            .get_mut(&family)
            .expect("egui's default families exist")
            .push(name.to_owned());
    }
}

/// The startup call's product: egui's defaults plus the system CJK
/// font, ready for `Context::set_fonts`.
pub fn font_definitions() -> egui::FontDefinitions {
    let mut definitions = egui::FontDefinitions::default();
    if let Some(bytes) = load_cjk_font() {
        insert_cjk_fallback(&mut definitions, bytes, "system_cjk");
    }
    definitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fallback_rides_last_in_both_families() {
        // FontData does not parse eagerly (rasterization does), so a
        // stand-in byte vector exercises the insertion shape: last in
        // the family lists, present in the data map.
        let mut definitions = egui::FontDefinitions::default();
        insert_cjk_fallback(&mut definitions, vec![1, 2, 3], "test_cjk");
        for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            let names = &definitions.families[&family];
            assert_eq!(
                names.last(),
                Some(&"test_cjk".to_owned()),
                "the fallback is consulted last, so the defaults win for the glyphs they have"
            );
        }
        assert!(definitions.font_data.contains_key("test_cjk"));
    }

    #[test]
    fn a_desktop_system_yields_a_font_and_a_bare_container_may_not() {
        // Platform reality (CODING_STANDARDS): this test's meaningful
        // half runs where a desktop user lives — macOS and Windows
        // always ship CJK fonts, and every Linux desktop ships at least
        // one of the candidates. A stripped headless CI container may
        // carry no fonts at all; there the trivial half is the honest
        // answer (None, and the GUI runs on the defaults).
        match load_cjk_font() {
            Some(bytes) => {
                assert!(
                    !bytes.is_empty(),
                    "a found candidate is read whole, not a stub"
                );
            }
            None => {
                let found = cjk_font_candidates()
                    .iter()
                    .any(|path| std::path::Path::new(path).exists());
                assert!(
                    !found,
                    "a candidate exists but did not read — that is a bug, not a fontless machine"
                );
            }
        }
    }

    #[test]
    fn every_platform_has_a_candidate_list() {
        assert!(
            !cjk_font_candidates().is_empty(),
            "the red line's foundation must try something on every platform"
        );
    }
}
