# TradingAgents Deployment Status

Last updated: 2026-04-01

## Overall Status

Production deployment is operational on Railway.

## Verified Items

- Railway service is running in the production environment.
- APScheduler is active and monitoring scheduled jobs.
- Production schedule is configured for Monday-Friday at 21:05 UTC.
- Telegram notifications are enabled and verified.
- Alpaca paper trading connectivity is working.
- Manual and scheduled execution paths completed successfully during validation.

## Latest Confirmed Execution

Test execution completed successfully on 2026-04-01.

- Ticker analyzed: SPY
- Final signal: HOLD
- Alpaca account status: ACTIVE
- Report delivery: Telegram message sent successfully
- Telegram confirmation: message_id=27

## Production Configuration

- SCHEDULE_HOUR_UTC=21
- SCHEDULE_MINUTE_UTC=5
- LOG_LEVEL=INFO
- TELEGRAM_ENABLED=true

## Operational Notes

- The scheduler was temporarily moved to a test window during validation and then restored to the production schedule.
- Existing repo changes under eval_results were left untouched and are not part of this status update.
- Next automatic run is expected on the next Monday-Friday window at 21:05 UTC.

## Relevant Components

- worker.py: scheduler startup, trading job orchestration, Telegram summary sending
- alpaca_trade/alpaca_executor.py: account access and trade execution handling
- tradingagents/dataflows/y_finance.py: market data dependency fix used during validation

## Summary

The Railway deployment, trading workflow, and Telegram notification flow have all been validated end-to-end and are ready for normal automated operation.