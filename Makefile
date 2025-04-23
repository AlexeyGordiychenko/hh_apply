clear_send_log:
	> send_applies.log
clear_reject_log:
	> process_rejection.log
clear_manual_log:
	> add_manual_applies.log

send_query_test: clear_send_log
	uv run send_applies.py -t -s query
send_similar_test: clear_send_log
	uv run send_applies.py -t -s similar
send_query: clear_send_log
	uv run send_applies.py -s query
	$(MAKE) get_manual_applies
	$(MAKE) get_skipped_applies
send_similar: clear_send_log
	uv run send_applies.py -s similar
	$(MAKE) get_manual_applies
	$(MAKE) get_skipped_applies

get_manual_applies:
	grep -i "process test" send_applies.log >> send_applies_manual_$$(date +%d%m).log || true
	grep -i "external apply required" send_applies.log >> send_applies_manual_$$(date +%d%m).log || true
get_skipped_applies:
	grep -i "SKIPPED due to blacklist words" send_applies.log | cut -d' ' -f9- | awk '{first=$$1; $$1=""; print substr($$0,2), first}' | sort -u >> send_applies_skipped_$$(date +%d%m).log || true

process_rejection: clear_reject_log
	uv run process_rejection.py

add_manual: clear_manual_log
	uv run add_manual_applies.py --date "2025-04-23 15:34:00+05"

add_manual_test: clear_manual_log
	uv run add_manual_applies.py -t --date "2025-04-23 15:34:00+05"