//! The Management Window's History Log view (GLOSSARY, ticket #15):
//! picking a Subscription opens its History Log — a bounded tail loaded
//! as one snapshot request over the channel, then live-following as the
//! daemon's append events arrive — `log --follow` GUI-ified. The same
//! thin split as the tray and window models: the model owns every word,
//! paint owns none. Content only ever arrives over the channel; the
//! History Log files stay daemon-private (ADR-0004) — nothing here
//! names a file path.
//!
//! The races this state machine pins down: an append event and the
//! snapshot answer can cross on the wire in either order (the daemon
//! reads its file at answer time; the append is announced the moment
//! the line is on disk), so appends heard while the request is in
//! flight are buffered and replayed through id dedup when the answer
//! lands — a line shows exactly once whichever way the race went. And
//! an outage swallows events, so the daemon's return re-requests the
//! tail instead of posing stale lines as live.

use echo_ipc::message::{Event, NotificationIntent};

/// The bounded snapshot's tail and the view's in-memory cap: one
/// number, so the opening content and the live appends obey the same
/// bound and a large History Log can never weigh on the UI (ticket #15).
pub const HISTORY_TAIL: u32 = 200;

/// The view's bound as a line count — [`HISTORY_TAIL`] again, one
/// vocabulary.
const MAX_LINES: usize = HISTORY_TAIL as usize;

/// Where the viewer stands for its picked Subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Nothing picked — the viewer shows its invitation.
    Idle,
    /// The snapshot request is out; appends heard meanwhile are buffered
    /// so the answer's read can never erase them.
    Loading,
    /// Following live: the tail is on screen and appends land in place.
    Live,
    /// The request failed or the link broke — named, never a silent
    /// blank. The lines already shown stay (history does not un-happen).
    Unavailable {
        /// What went wrong, in the viewer's own words.
        reason: String,
    },
}

/// One line of the viewer, derived from a History Log entry — the words
/// already chosen, ready to paint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineView {
    /// The entry's per-Device message id — the dedup key when a live
    /// append races the snapshot (ticket #15).
    pub id: i64,
    pub text: String,
}

/// The History Log view's whole world: which Subscription is picked,
/// where the follow stands, and the bounded lines on screen.
#[derive(Debug)]
pub struct HistoryView {
    selected: Option<String>,
    label: String,
    phase: Phase,
    lines: Vec<LineView>,
    /// Appends heard while the snapshot request was in flight: replayed
    /// through dedup when the answer lands, so a line the answer's read
    /// predates is never dropped (the append/snapshot race).
    inflight: Vec<LineView>,
}

impl Default for HistoryView {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryView {
    /// A viewer with nothing picked.
    pub fn new() -> Self {
        Self {
            selected: None,
            label: String::new(),
            phase: Phase::Idle,
            lines: Vec::new(),
            inflight: Vec::new(),
        }
    }

    /// Pick a Subscription: the view resets (fresh content for a fresh
    /// pick) and asks for a bounded tail of its History Log — the
    /// returned id is what the shell sends as the snapshot request over
    /// the channel. `None` means no request is needed (re-picking the
    /// Subscription already being followed, or already loading).
    pub fn select(&mut self, id: &str, label: &str) -> Option<String> {
        let already_loading = self.selected.as_deref() == Some(id) && self.phase == Phase::Loading;
        let already_live = self.selected.as_deref() == Some(id) && self.phase == Phase::Live;
        if already_loading || already_live {
            return None;
        }
        self.selected = Some(id.to_owned());
        self.label = label.to_owned();
        self.phase = Phase::Loading;
        self.lines.clear();
        self.inflight.clear();
        Some(id.to_owned())
    }

    /// The picked Subscription's id, for the row highlight.
    pub fn selected_id(&self) -> Option<&str> {
        self.selected.as_deref()
    }

    /// The snapshot answer landed: its entries become the lines, oldest
    /// last, and the view follows live. Appends buffered while the
    /// answer was in flight replay through dedup, so a line is shown
    /// exactly once whichever way the race went. An answer for another
    /// Subscription (a stale in-flight reply) is ignored.
    pub fn snapshot_loaded(&mut self, subscription_id: &str, entries: &[NotificationIntent]) {
        if self.selected.as_deref() != Some(subscription_id) {
            return;
        }
        self.lines = entries
            .iter()
            .map(|entry| LineView {
                id: entry.id,
                text: line_text(entry),
            })
            .collect();
        if self.lines.len() > MAX_LINES {
            self.lines.drain(..self.lines.len() - MAX_LINES);
        }
        let replayed = std::mem::take(&mut self.inflight);
        for line in replayed {
            self.land(line);
        }
        self.phase = Phase::Live;
    }

    /// The snapshot request failed — named, not blank; lines heard live
    /// stay visible.
    pub fn snapshot_failed(&mut self, subscription_id: &str, reason: &str) {
        if self.selected.as_deref() != Some(subscription_id) {
            return;
        }
        let replayed = std::mem::take(&mut self.inflight);
        for line in replayed {
            self.land(line);
        }
        self.phase = Phase::Unavailable {
            reason: reason.to_owned(),
        };
    }

    /// One daemon-pushed event. A History Log append for the picked
    /// Subscription lands in place — no request, no reopen (the live
    /// half of `log --follow`). Appends heard while the snapshot request
    /// is in flight are buffered for the answer's replay; anything else
    /// (other Subscriptions, stream flips) is not this view's food.
    pub fn apply_append(&mut self, event: &Event) {
        let Event::HistoryAppended {
            subscription_id,
            entry,
            ..
        } = event
        else {
            return;
        };
        if self.selected.as_deref() != Some(subscription_id.as_str()) {
            return;
        }
        let line = LineView {
            id: entry.id,
            text: line_text(entry),
        };
        match self.phase {
            // The answer's read may predate this line: buffer it for the
            // replay instead of painting it into an answer about to
            // replace the screen.
            Phase::Loading => self.buffer(line),
            Phase::Live => self.land(line),
            // No channel, no appends — and a re-request refreshes
            // whatever a stray one might have carried.
            Phase::Idle | Phase::Unavailable { .. } => {}
        }
    }

    /// The channel broke: the absence is named and the follow stops
    /// pretending. The lines stay — they are already-delivered history.
    pub fn connection_lost(&mut self) {
        if self.selected.is_some() {
            self.phase = Phase::Unavailable {
                reason: "the daemon is away".to_owned(),
            };
        }
    }

    /// The daemon is back (the connection loop's opening snapshot —
    /// fresh authority): the picked Subscription's tail is requested
    /// again, because appends that landed during the outage never
    /// reached this view. Returns the id the shell sends the request
    /// for, `None` when nothing is picked.
    pub fn daemon_back(&mut self) -> Option<String> {
        let selected = self.selected.clone()?;
        self.phase = Phase::Loading;
        Some(selected)
    }

    /// The section heading: what the viewer is showing.
    pub fn heading(&self) -> String {
        match self.selected.as_deref() {
            Some(_) => format!("History Log — {}", self.label),
            None => "History Log".to_owned(),
        }
    }

    /// The status banner's words, `None` when the follow is quiet and
    /// the lines speak for themselves.
    pub fn banner(&self) -> Option<String> {
        match &self.phase {
            Phase::Idle => Some("Pick a Subscription above to browse its History Log.".to_owned()),
            Phase::Loading => Some("Loading the recent History Log…".to_owned()),
            Phase::Unavailable { reason } => Some(format!("History Log unavailable — {reason}")),
            Phase::Live => None,
        }
    }

    /// The words for an empty view that is none of the banner's
    /// business (a live follow of a Subscription with no lines yet);
    /// `None` when there is nothing worth saying.
    pub fn empty_words(&self) -> Option<String> {
        match self.phase {
            Phase::Live if self.lines.is_empty() => {
                Some("No History Log lines yet — a Push lands here when one arrives.".to_owned())
            }
            _ => None,
        }
    }

    /// The lines to paint, oldest last, bounded — ready as they are.
    pub fn lines(&self) -> &[LineView] {
        &self.lines
    }

    /// Land one append line at the bottom, deduped by id (the replayed
    /// event of a line the snapshot already carried must not duplicate
    /// it) and bounded (the oldest line drops when the cap is reached —
    /// the same bound the snapshot obeyed).
    fn land(&mut self, line: LineView) {
        if self.lines.iter().any(|shown| shown.id == line.id) {
            return;
        }
        self.lines.push(line);
        if self.lines.len() > MAX_LINES {
            self.lines.remove(0);
        }
    }

    /// Buffer an append heard while the snapshot request was in flight;
    /// deduped against what is already buffered and bounded the same
    /// way (a huge burst while loading stays a bounded view).
    fn buffer(&mut self, line: LineView) {
        if self.inflight.iter().any(|held| held.id == line.id) {
            return;
        }
        self.inflight.push(line);
        if self.inflight.len() > MAX_LINES {
            self.inflight.remove(0);
        }
    }
}

/// One entry's line, shaped like the CLI's `log` output (the same
/// facts in the same order: the per-Device message id, the
/// server-assigned RFC 3339 UTC time, the alert parts joined, the
/// resolved click action) — the viewer reads like the command it
/// GUI-ifies (ticket #15).
fn line_text(entry: &NotificationIntent) -> String {
    let when = match time::OffsetDateTime::from_unix_timestamp(entry.created_at) {
        Ok(stamp) => stamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| entry.created_at.to_string()),
        Err(_) => entry.created_at.to_string(),
    };
    let alert: Vec<&str> = [
        entry.title.as_str(),
        entry.subtitle.as_str(),
        entry.body.as_str(),
    ]
    .into_iter()
    .filter(|part| !part.is_empty())
    .collect();
    let action = match &entry.click_action {
        Some(echo_ipc::message::ClickAction::OpenUrl { url }) => format!(" [open {url}]"),
        Some(echo_ipc::message::ClickAction::Copy { .. }) => " [copy]".to_owned(),
        None => String::new(),
    };
    if alert.is_empty() {
        format!("#{} {}{}", entry.id, when, action)
    } else {
        format!("#{} {} {}{}", entry.id, when, alert.join(" / "), action)
    }
}

/// The viewer's face: heading, banner, lines — thin by contract, the
/// model owns every word. Called by the window's render under the
/// Subscription list.
pub fn render(ui: &mut egui::Ui, view: &HistoryView) {
    ui.add_space(4.0);
    ui.strong(view.heading());
    if let Some(banner) = view.banner() {
        let mut text = egui::RichText::new(banner);
        if matches!(view.phase, Phase::Unavailable { .. }) {
            // The same color the window's banner names the absence
            // with: one vocabulary, two surfaces.
            text = text.color(egui::Color32::LIGHT_RED);
        }
        ui.label(text);
    }
    let lines = view.lines();
    if lines.is_empty() {
        if let Some(words) = view.empty_words() {
            ui.label(words);
        }
        return;
    }
    // Newest last, stuck to the bottom: the line that just landed is
    // the one the user is here for (`log --follow`'s scrolling tail).
    // The bound (HISTORY_TAIL) keeps the loop small — no virtualization
    // needed.
    egui::ScrollArea::vertical()
        .stick_to_bottom(true)
        .show(ui, |ui| {
            for line in lines {
                ui.label(&line.text);
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: i64, body: &str) -> NotificationIntent {
        NotificationIntent {
            id,
            title: "the title".to_owned(),
            subtitle: String::new(),
            body: body.to_owned(),
            click_action: None,
            created_at: 1_700_000_000,
        }
    }

    fn append(id: i64, body: &str) -> Event {
        Event::HistoryAppended {
            subscription_id: "tok-a".to_owned(),
            label: "desktop".to_owned(),
            entry: entry(id, body),
        }
    }

    #[test]
    fn a_fresh_viewer_shows_its_invitation_and_nothing_else() {
        let view = HistoryView::new();
        assert_eq!(view.selected_id(), None);
        assert!(view.lines().is_empty());
        assert_eq!(view.heading(), "History Log");
        assert_eq!(
            view.banner(),
            Some("Pick a Subscription above to browse its History Log.".to_owned())
        );
    }

    #[test]
    fn picking_a_subscription_resets_the_view_and_asks_for_its_tail() {
        let mut view = HistoryView::new();
        let ask = view.select("tok-a", "desktop");
        assert_eq!(
            ask,
            Some("tok-a".to_owned()),
            "the shell sends the snapshot request for this id"
        );
        assert_eq!(view.selected_id(), Some("tok-a"));
        assert_eq!(view.heading(), "History Log — desktop");
        assert_eq!(view.banner().unwrap(), "Loading the recent History Log…");

        // Content from an earlier pick never survives the switch.
        view.snapshot_loaded("tok-a", &[entry(1, "first"), entry(2, "second")]);
        assert_eq!(view.lines().len(), 2);
        let re_ask = view.select("tok-b", "kitchen");
        assert_eq!(re_ask, Some("tok-b".to_owned()));
        assert!(
            view.lines().is_empty(),
            "switching Subscriptions resets the view"
        );
        assert_eq!(view.heading(), "History Log — kitchen");
    }

    #[test]
    fn re_picking_the_subscription_already_following_does_not_re_ask() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        assert_eq!(view.select("tok-a", "desktop"), None, "the follow is live");
    }

    #[test]
    fn re_picking_a_failed_view_retries_the_request() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_failed("tok-a", "the link broke");
        assert_eq!(
            view.select("tok-a", "desktop"),
            Some("tok-a".to_owned()),
            "an unavailable view tries again on demand"
        );
    }

    #[test]
    fn the_snapshot_becomes_lines_oldest_last_and_shaped_like_the_cli_output() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first"), entry(2, "second")]);
        let lines = view.lines();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].text.starts_with("#1 2023-11-14T22:13:20Z"));
        assert!(lines[1].text.starts_with("#2 2023-11-14T22:13:20Z"));
        assert!(
            lines[0].text.contains("the title / first"),
            "the alert parts join as the CLI joins them: {}",
            lines[0].text
        );
        assert_eq!(view.banner(), None, "a quiet follow needs no banner");
    }

    #[test]
    fn appends_land_in_place_while_following() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        view.apply_append(&append(2, "second"));
        let lines = view.lines();
        assert_eq!(lines.len(), 2, "the append lands without a request");
        assert_eq!(lines[1].id, 2);
        assert!(lines[1].text.contains("second"));
    }

    #[test]
    fn an_append_for_another_subscription_is_not_this_view_s_food() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        view.apply_append(&Event::HistoryAppended {
            subscription_id: "tok-b".to_owned(),
            label: "kitchen".to_owned(),
            entry: entry(9, "elsewhere"),
        });
        assert_eq!(view.lines().len(), 1, "the other Subscription is ignored");
    }

    #[test]
    fn an_append_heard_before_any_pick_is_ignored() {
        let mut view = HistoryView::new();
        view.apply_append(&append(1, "first"));
        assert!(view.lines().is_empty());
    }

    #[test]
    fn an_append_that_races_the_snapshot_shows_exactly_once_whichever_way_it_lands() {
        // Race, event first: the append is heard while the snapshot
        // request is in flight, and the answer's read predates it — the
        // answer must not erase the line.
        let mut event_first = HistoryView::new();
        event_first.select("tok-a", "desktop");
        event_first.apply_append(&append(5, "racy"));
        event_first.snapshot_loaded("tok-a", &[entry(1, "first")]);
        let lines = event_first.lines();
        assert_eq!(lines.len(), 2, "the racy line survives the answer");
        assert_eq!(lines[0].id, 1);
        assert_eq!(lines[1].id, 5);

        // Race, answer first: the snapshot carries the line and the
        // replayed append event must not duplicate it.
        let mut answer_first = HistoryView::new();
        answer_first.select("tok-a", "desktop");
        answer_first.snapshot_loaded("tok-a", &[entry(1, "first"), entry(5, "racy")]);
        answer_first.apply_append(&append(5, "racy"));
        assert_eq!(
            answer_first.lines().len(),
            2,
            "the replayed append is deduped by id"
        );
    }

    #[test]
    fn appends_beyond_the_bound_drop_the_oldest_line() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        let entries: Vec<NotificationIntent> = (0..MAX_LINES as i64)
            .map(|id| entry(id, "filler"))
            .collect();
        view.snapshot_loaded("tok-a", &entries);
        assert_eq!(view.lines().len(), MAX_LINES);
        view.apply_append(&append(MAX_LINES as i64, "the newest"));
        let lines = view.lines();
        assert_eq!(lines.len(), MAX_LINES, "the view stays bounded");
        assert_eq!(lines[0].id, 1, "the oldest filler is dropped");
        assert_eq!(
            lines.last().unwrap().id,
            MAX_LINES as i64,
            "the newest lands at the bottom"
        );
    }

    #[test]
    fn a_failed_request_is_named_and_keeps_the_lines_heard_live() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.apply_append(&append(5, "racy"));
        view.snapshot_failed("tok-a", "the link broke");
        assert_eq!(
            view.banner(),
            Some("History Log unavailable — the link broke".to_owned())
        );
        assert_eq!(view.lines().len(), 1, "lines heard live stay visible");
    }

    #[test]
    fn a_failure_for_another_subscription_is_ignored() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        view.snapshot_failed("tok-b", "the link broke");
        assert_eq!(view.banner(), None, "the follow is untouched");
        assert_eq!(view.lines().len(), 1);
    }

    #[test]
    fn losing_the_link_names_the_absence_and_keeps_the_history() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        view.connection_lost();
        assert_eq!(
            view.banner(),
            Some("History Log unavailable — the daemon is away".to_owned())
        );
        assert_eq!(view.lines().len(), 1, "history does not un-happen");
    }

    #[test]
    fn the_daemons_return_re_requests_the_picked_tail() {
        let mut view = HistoryView::new();
        view.select("tok-a", "desktop");
        view.snapshot_loaded("tok-a", &[entry(1, "first")]);
        view.connection_lost();
        let re_ask = view.daemon_back();
        assert_eq!(
            re_ask,
            Some("tok-a".to_owned()),
            "appends that landed during the outage never reached this view — re-request"
        );
        assert_eq!(view.banner().unwrap(), "Loading the recent History Log…");
    }

    #[test]
    fn the_daemons_return_means_nothing_to_an_idle_viewer() {
        let mut view = HistoryView::new();
        assert_eq!(view.daemon_back(), None);
    }
}
