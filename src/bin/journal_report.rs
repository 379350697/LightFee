use std::{env, fs, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use lightfee::{analyze_journal_records, JournalRecord};

fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err(anyhow!(
            "usage: cargo run --bin journal_report -- [--json] <event-log-path>"
        ));
    }

    let json_output = args.iter().any(|arg| arg == "--json");
    let path_arg = args
        .iter()
        .find(|arg| arg.as_str() != "--json")
        .ok_or_else(|| anyhow!("missing event log path"))?;
    let path = PathBuf::from(path_arg);
    let records = read_records(&path)?;
    let report = analyze_journal_records(&records);

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!(
        "journal_report path={} total_records={} run_count={}",
        path.display(),
        report.total_records,
        report.run_count
    );

    for (venue, stats) in &report.venue_stats {
        println!(
            "venue={} submitted_orders={} filled_orders={} failed_orders={} failure_rate_pct={:.2} latency_p50_ms={:?} latency_p95_ms={:?} latency_p99_ms={:?} latency_max_ms={:?} total_fee_quote={:.8}",
            venue,
            stats.submitted_orders,
            stats.filled_orders,
            stats.failed_orders,
            stats.failure_rate_pct,
            stats.latency_ms_p50,
            stats.latency_ms_p95,
            stats.latency_ms_p99,
            stats.latency_ms_max,
            stats.total_fee_quote,
        );
    }

    for (kind, count) in &report.recovery_counts {
        println!("recovery kind={} count={}", kind, count);
    }

    for (reason, count) in &report.fail_closed_reason_counts {
        println!("fail_closed reason={} count={}", reason, count);
    }

    for (reason, count) in &report.optimization_stats.runtime_gate_block_counts {
        println!("runtime_gate reason={} count={}", reason, count);
    }

    for (reason, count) in &report.optimization_stats.order_block_reason_counts {
        println!("order_block reason={} count={}", reason, count);
    }

    for (venue, count) in &report.optimization_stats.venue_cooldown_counts {
        println!("cooldown venue={} count={}", venue, count);
    }

    for (first_leg, count) in &report.optimization_stats.first_leg_counts {
        println!("entry_plan first_leg={} count={}", first_leg, count);
    }

    for (factor, count) in &report.optimization_stats.first_leg_dominant_factor_counts {
        println!("entry_plan dominant_factor={} count={}", factor, count);
    }

    for (error, count) in &report.optimization_stats.order_error_counts {
        println!("order_error category={} count={}", error, count);
    }

    for (reason, count) in &report.optimization_stats.no_entry_reason_counts {
        println!("no_entry reason={} count={}", reason, count);
    }

    for (reason, count) in &report.optimization_stats.no_entry_blocked_reason_counts {
        println!("no_entry_blocked reason={} count={}", reason, count);
    }

    for (advisory, count) in &report.optimization_stats.no_entry_advisory_counts {
        println!("no_entry_advisory advisory={} count={}", advisory, count);
    }

    for (key, count) in &report.optimization_stats.no_entry_checklist_fail_counts {
        println!("no_entry_checklist_failed key={} count={}", key, count);
    }

    for (blocker, count) in &report.optimization_stats.no_entry_selection_blocker_counts {
        println!(
            "no_entry_selection_blocker blocker={} count={}",
            blocker, count
        );
    }

    for recommendation in &report.recommendations {
        println!(
            "recommendation priority={} category={} title={} summary={} evidence={:?}",
            recommendation.priority,
            recommendation.category,
            recommendation.title,
            recommendation.summary,
            recommendation.evidence,
        );
    }

    for replay in report.trade_replays.iter().rev().take(10).rev() {
        println!(
            "trade position_id={} state={} pair_id={:?} symbol={:?} total_fee_quote={:.8} warnings={} errors={}",
            replay.position_id,
            replay.state,
            replay.pair_id,
            replay.symbol,
            replay.total_fee_quote,
            replay.warnings.len(),
            replay.errors.len(),
        );
        if let Some(plan) = &replay.entry_order_plan {
            println!(
                "trade_plan position_id={} plan={}",
                replay.position_id, plan
            );
        }
        if let Some(entry) = &replay.entry_latency_summary {
            println!(
                "trade_entry_latency position_id={} summary={}",
                replay.position_id, entry
            );
        }
        if let Some(opened) = &replay.opened_position {
            println!(
                "trade_opened position_id={} summary={}",
                replay.position_id, opened
            );
        }
        for partial in &replay.partial_close_summaries {
            println!(
                "trade_partial_close position_id={} summary={}",
                replay.position_id, partial
            );
        }
        if let Some(exit) = &replay.exit_latency_summary {
            println!(
                "trade_exit_latency position_id={} summary={}",
                replay.position_id, exit
            );
        }
        if let Some(close) = &replay.close_summary {
            println!(
                "trade_closed position_id={} summary={}",
                replay.position_id, close
            );
        }
        for leg in &replay.order_legs {
            println!(
                "trade_leg position_id={} stage={} outcome={} venue={:?} side={:?} requested_qty={:?} executed_qty={:?} avg_price={:?} fee_quote={:?} local_roundtrip_ms={:?} error={:?}",
                replay.position_id,
                leg.stage,
                leg.outcome,
                leg.venue,
                leg.side,
                leg.requested_quantity,
                leg.executed_quantity,
                leg.average_price,
                leg.fee_quote,
                leg.local_roundtrip_ms,
                leg.error,
            );
        }
        for warning in &replay.warnings {
            println!(
                "trade_warning position_id={} warning={}",
                replay.position_id, warning
            );
        }
        for error in &replay.errors {
            println!(
                "trade_error position_id={} error={}",
                replay.position_id, error
            );
        }
    }

    Ok(())
}

fn read_records(path: &PathBuf) -> Result<Vec<JournalRecord>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read journal {}", path.display()))?;
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<JournalRecord>(line)
                .with_context(|| format!("failed to parse journal record in {}", path.display()))
        })
        .collect()
}
