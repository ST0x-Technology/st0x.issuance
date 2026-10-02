//! The US extended-hours session the fill rate monitor is awake for.
//!
//! The session, in `America/New_York` wall-clock time, is Monday to Friday from
//! 04:00 up to but excluding 20:00, minus NYSE full holidays. Wall-clock time
//! matters because the UTC offset changes with daylight saving; the clock
//! changes happen at 02:00 on a Sunday, so a session day never contains one and
//! elapsed time since the open is plain wall-clock subtraction.

use chrono::{
    DateTime, Datelike, NaiveDate, TimeDelta, Timelike, Utc, Weekday,
};
use chrono_tz::America::New_York;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::ops::RangeInclusive;

const SECONDS_PER_HOUR: u32 = 3600;

/// 04:00 as seconds since local midnight.
const OPEN_SECONDS: u32 = 4 * SECONDS_PER_HOUR;

/// 20:00 as seconds since local midnight; the first closed second.
const CLOSE_SECONDS: u32 = 20 * SECONDS_PER_HOUR;

/// Hours between the open and the close. A window longer than this can never
/// fill inside one session.
pub(crate) const SESSION_HOURS: u32 =
    (CLOSE_SECONDS - OPEN_SECONDS) / SECONDS_PER_HOUR;

/// Holiday list checked in next to this module; see the file for its format.
const EMBEDDED_HOLIDAYS: &str = include_str!("nyse_holidays.txt");

/// NYSE full-day holidays for a bounded range of years.
pub(crate) struct NyseCalendar {
    covered_years: RangeInclusive<i32>,
    holidays: BTreeSet<NaiveDate>,
}

/// Where `now` falls relative to the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState {
    /// Overnight, a weekend, or a full holiday.
    Closed,
    /// Open for less than the window, so the window is not yet full.
    WarmingUp,
    /// Open for at least the window. The window starts at `window_start` and
    /// lies wholly inside the current session.
    WindowFull { window_start: DateTime<Utc> },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CalendarError {
    #[error("holiday list has no `covers FIRST_YEAR LAST_YEAR` line")]
    MissingCoverage,
    #[error("line {line}: a second `covers` line")]
    DuplicateCoverage { line: usize },
    #[error("line {line}: `covers {first} {last}` is an empty range")]
    EmptyCoverage { line: usize, first: i32, last: i32 },
    #[error(
        "line {line}: expected `covers FIRST_YEAR LAST_YEAR` or `YYYY-MM-DD name`, got `{text}`"
    )]
    MalformedLine { line: usize, text: String },
    #[error("line {line}: {date} is outside the covered years {first}-{last}")]
    OutsideCoverage { line: usize, date: NaiveDate, first: i32, last: i32 },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionError {
    #[error("{date} is outside the years the holiday list covers")]
    UncoveredDate { date: NaiveDate },
    #[error("a window of {window_hours} hours does not fit a timestamp")]
    WindowOutOfRange { window_hours: u32 },
}

impl NyseCalendar {
    /// The holiday list checked into the repository.
    pub(crate) fn embedded() -> Result<Self, CalendarError> {
        Self::parse(EMBEDDED_HOLIDAYS)
    }

    fn parse(text: &str) -> Result<Self, CalendarError> {
        let mut coverage = None;
        let mut dated_lines = Vec::new();

        for (index, raw_line) in text.lines().enumerate() {
            let line = index + 1;
            let trimmed = raw_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let malformed = || CalendarError::MalformedLine {
                line,
                text: trimmed.to_owned(),
            };
            let mut words = trimmed.split_whitespace();

            if words.next() == Some("covers") {
                let (Some(first), Some(last), None) =
                    (words.next(), words.next(), words.next())
                else {
                    return Err(malformed());
                };
                let first: i32 = first.parse().map_err(|_| malformed())?;
                let last: i32 = last.parse().map_err(|_| malformed())?;

                if coverage.is_some() {
                    return Err(CalendarError::DuplicateCoverage { line });
                }
                if first > last {
                    return Err(CalendarError::EmptyCoverage {
                        line,
                        first,
                        last,
                    });
                }
                coverage = Some(first..=last);
                continue;
            }

            // The first word was consumed by the `covers` check above, so the
            // date is re-read from the start of the line.
            let (date_text, name) = trimmed
                .split_once(char::is_whitespace)
                .ok_or_else(malformed)?;
            if name.trim().is_empty() {
                return Err(malformed());
            }
            let date = NaiveDate::parse_from_str(date_text, "%Y-%m-%d")
                .map_err(|_| malformed())?;
            dated_lines.push((line, date));
        }

        let covered_years = coverage.ok_or(CalendarError::MissingCoverage)?;

        if let Some(&(line, date)) = dated_lines
            .iter()
            .find(|(_, date)| !covered_years.contains(&date.year()))
        {
            return Err(CalendarError::OutsideCoverage {
                line,
                date,
                first: *covered_years.start(),
                last: *covered_years.end(),
            });
        }

        Ok(Self {
            covered_years,
            holidays: dated_lines.into_iter().map(|(_, date)| date).collect(),
        })
    }

    /// Whether `date` is inside the years the list covers, i.e. whether a
    /// date missing from it can be trusted to be a trading day.
    pub(crate) fn covers(&self, date: NaiveDate) -> bool {
        self.covered_years.contains(&date.year())
    }

    /// Places `now` relative to the session for a window of `window_hours`.
    pub(crate) fn session_state(
        &self,
        now: DateTime<Utc>,
        window_hours: NonZeroU32,
    ) -> Result<SessionState, SessionError> {
        let local = now.with_timezone(&New_York);
        let date = local.date_naive();

        if !self.covers(date) {
            return Err(SessionError::UncoveredDate { date });
        }

        let seconds = local.num_seconds_from_midnight();
        let is_weekend = matches!(local.weekday(), Weekday::Sat | Weekday::Sun);
        let is_open_hours = (OPEN_SECONDS..CLOSE_SECONDS).contains(&seconds);

        if is_weekend || !is_open_hours || self.holidays.contains(&date) {
            return Ok(SessionState::Closed);
        }

        let window_seconds =
            u64::from(window_hours.get()) * u64::from(SECONDS_PER_HOUR);
        if u64::from(seconds - OPEN_SECONDS) < window_seconds {
            return Ok(SessionState::WarmingUp);
        }

        let window_start = TimeDelta::try_hours(i64::from(window_hours.get()))
            .and_then(|window| now.checked_sub_signed(window))
            .ok_or_else(|| SessionError::WindowOutOfRange {
                window_hours: window_hours.get(),
            })?;

        Ok(SessionState::WindowFull { window_start })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use std::num::NonZeroU32;

    use super::{
        CalendarError, NyseCalendar, SESSION_HOURS, SessionError, SessionState,
    };

    fn calendar() -> NyseCalendar {
        NyseCalendar::embedded().unwrap()
    }

    fn window(hours: u32) -> NonZeroU32 {
        NonZeroU32::new(hours).unwrap()
    }

    /// New York wall-clock time as the instant it names.
    fn new_york(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
    ) -> DateTime<Utc> {
        New_York
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    fn state_at(now: DateTime<Utc>, window_hours: u32) -> SessionState {
        calendar().session_state(now, window(window_hours)).unwrap()
    }

    #[test]
    fn saturday_and_sunday_are_closed_all_day() {
        // 2026-10-03 is a Saturday, 2026-10-04 a Sunday.
        for (day, hour) in [(3, 4), (3, 12), (3, 19), (4, 4), (4, 12), (4, 19)]
        {
            assert_eq!(
                state_at(new_york(2026, 10, day, hour, 0), 6),
                SessionState::Closed,
                "2026-10-{day:02} {hour:02}:00 falls on a weekend"
            );
        }
    }

    #[test]
    fn overnight_gap_is_closed_and_the_close_is_exclusive() {
        // 2026-10-05 is a Monday.
        assert_eq!(
            state_at(new_york(2026, 10, 5, 3, 59), 6),
            SessionState::Closed
        );
        assert_eq!(
            state_at(new_york(2026, 10, 6, 0, 0), 6),
            SessionState::Closed
        );
        assert_eq!(
            state_at(new_york(2026, 10, 5, 20, 0), 6),
            SessionState::Closed,
            "20:00 is the first closed minute"
        );
        assert!(
            matches!(
                state_at(new_york(2026, 10, 5, 19, 59), 6),
                SessionState::WindowFull { .. }
            ),
            "19:59 is still inside the session"
        );
    }

    #[test]
    fn full_holidays_are_closed_even_at_midday() {
        for (month, day, name) in [
            (7, 3, "Independence Day observed, a Friday"),
            (9, 7, "Labor Day"),
            (11, 26, "Thanksgiving"),
            (12, 25, "Christmas"),
        ] {
            assert_eq!(
                state_at(new_york(2026, month, day, 12, 0), 6),
                SessionState::Closed,
                "{name} is a full holiday"
            );
        }
    }

    #[test]
    fn the_day_after_thanksgiving_is_open_because_early_closes_are_ignored() {
        assert_eq!(
            state_at(new_york(2026, 11, 27, 4, 30), 6),
            SessionState::WarmingUp
        );
        assert!(matches!(
            state_at(new_york(2026, 11, 27, 19, 0), 6),
            SessionState::WindowFull { .. }
        ));
    }

    #[test]
    fn a_session_open_for_less_than_the_window_is_warming_up() {
        assert_eq!(
            state_at(new_york(2026, 10, 5, 4, 0), 6),
            SessionState::WarmingUp,
            "the open itself is the start of the warm-up"
        );
        assert_eq!(
            state_at(new_york(2026, 10, 5, 9, 59), 6),
            SessionState::WarmingUp
        );
    }

    #[test]
    fn the_window_is_full_from_exactly_one_window_after_the_open() {
        let now = new_york(2026, 10, 5, 10, 0);

        assert_eq!(
            state_at(now, 6),
            SessionState::WindowFull {
                window_start: new_york(2026, 10, 5, 4, 0)
            },
            "with the default window the first page is at 10:00"
        );
    }

    #[test]
    fn mondays_window_never_reaches_back_to_fridays_session() {
        // Friday evening was inside a session, but Monday morning's window is
        // not full until Monday itself has been open for the whole window.
        assert!(matches!(
            state_at(new_york(2026, 10, 2, 19, 0), 6),
            SessionState::WindowFull { .. }
        ));
        assert_eq!(
            state_at(new_york(2026, 10, 5, 5, 0), 6),
            SessionState::WarmingUp
        );

        let SessionState::WindowFull { window_start } =
            state_at(new_york(2026, 10, 5, 12, 0), 6)
        else {
            panic!("a session open for eight hours has a full window");
        };
        assert_eq!(window_start, new_york(2026, 10, 5, 6, 0));
    }

    #[test]
    fn a_window_as_long_as_the_session_never_fills_inside_it() {
        assert_eq!(
            state_at(new_york(2026, 10, 5, 19, 59), SESSION_HOURS),
            SessionState::WarmingUp
        );
        assert_eq!(
            state_at(new_york(2026, 10, 5, 20, 0), SESSION_HOURS),
            SessionState::Closed
        );
    }

    #[test]
    fn the_open_follows_daylight_saving_time() {
        // 2026-03-09 is the Monday after the clocks went forward, and
        // 2026-11-02 the Monday after they went back.
        for (month, day) in [(3, 9), (11, 2)] {
            assert_eq!(
                state_at(new_york(2026, month, day, 10, 0), 6),
                SessionState::WindowFull {
                    window_start: new_york(2026, month, day, 4, 0)
                },
                "2026-{month:02}-{day:02}"
            );
        }
    }

    #[test]
    fn a_date_outside_the_covered_years_is_an_error_not_a_trading_day() {
        let result =
            calendar().session_state(new_york(2029, 7, 4, 12, 0), window(6));

        assert!(matches!(
            result,
            Err(SessionError::UncoveredDate { date })
                if date == NaiveDate::from_ymd_opt(2029, 7, 4).unwrap()
        ));
    }

    #[test]
    fn parse_reads_coverage_holidays_and_skips_comments() {
        let calendar = NyseCalendar::parse(
            "# comment\n\ncovers 2030 2031\n2030-01-01 New Year's Day\n",
        )
        .unwrap();

        assert!(calendar.covers(NaiveDate::from_ymd_opt(2031, 6, 1).unwrap()));
        assert!(!calendar.covers(NaiveDate::from_ymd_opt(2032, 1, 1).unwrap()));
        assert_eq!(
            calendar
                .session_state(new_york(2030, 1, 1, 12, 0), window(6))
                .unwrap(),
            SessionState::Closed
        );
    }

    #[test]
    fn parse_rejects_a_list_without_coverage() {
        assert!(matches!(
            NyseCalendar::parse("2030-01-01 New Year's Day\n"),
            Err(CalendarError::MissingCoverage)
        ));
    }

    #[test]
    fn parse_rejects_a_holiday_outside_the_covered_years() {
        assert!(matches!(
            NyseCalendar::parse(
                "covers 2030 2030\n2031-01-01 New Year's Day\n"
            ),
            Err(CalendarError::OutsideCoverage { line: 2, .. })
        ));
    }

    #[test]
    fn parse_rejects_malformed_lines_and_bad_coverage() {
        for text in [
            "covers 2030\n",
            "covers twenty thirty\n",
            "covers 2030 2031\n2030-13-01 Nonsense\n",
            "covers 2030 2031\n2030-01-01\n",
        ] {
            assert!(
                matches!(
                    NyseCalendar::parse(text),
                    Err(CalendarError::MalformedLine { .. })
                ),
                "{text:?} must be refused"
            );
        }
        assert!(matches!(
            NyseCalendar::parse("covers 2031 2030\n"),
            Err(CalendarError::EmptyCoverage { line: 1, .. })
        ));
        assert!(matches!(
            NyseCalendar::parse("covers 2030 2031\ncovers 2030 2031\n"),
            Err(CalendarError::DuplicateCoverage { line: 2 })
        ));
    }

    /// The list must always reach into next year, so a stale list fails CI a
    /// full year before it would start treating holidays as trading days.
    #[test]
    fn embedded_list_covers_this_year_and_next() {
        let today = Utc::now().with_timezone(&New_York).date_naive();
        let next_year = NaiveDate::from_ymd_opt(today.year() + 1, 12, 31)
            .expect("December 31 exists in every year");

        let calendar = calendar();

        assert!(
            calendar.covers(today),
            "nyse_holidays.txt does not cover today ({today}); add this \
             year's NYSE holidays"
        );
        assert!(
            calendar.covers(next_year),
            "nyse_holidays.txt does not cover next year ({next_year}); add \
             next year's NYSE holidays from \
             https://www.nyse.com/trade/hours-calendars"
        );
    }

    #[test]
    fn embedded_list_holds_only_weekdays() {
        // A holiday on a weekend is a transcription slip: the market is closed
        // anyway and the observed day was probably meant.
        let calendar = calendar();

        assert!(!calendar.holidays.is_empty());
        for holiday in &calendar.holidays {
            assert!(
                holiday.weekday().number_from_monday() <= 5,
                "{holiday} is on a weekend"
            );
        }
    }
}
