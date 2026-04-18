# Parser Observations

## Observed

Based on a review of log fixtures and explicit requirements, specific domain log markers define the lifecycle, success, and constraints of tasks in this companion.

### Validated Log Markers
The following log markers exist within the execution environments and are used to build domain insights:

1. **`summary blocks`**:
   * Structural completion metrics from the parser.
   * Example:
     ```
     |--------------------------------------
     |              Статистика              
     |--------------------------------------
     | Товаров всего парсере   > 144963
     | Резюме: ✅
     ```
2. **`auto_stop`**:
   * Deliberate or constraint-based stop signal. Example string: `Exception: error_auto_stop (6) is reached`
3. **`429 ban`**:
   * Blocked requests handled by the internal Crawllib profiles. Example strings: `Got ban status code 429...` or `Ban status code (429)`.
4. **`cancelled`**:
   * A task halted outside natural flow. Logs often abort sharply cleanly if handled by Crawlab supervisor (`{'status': 'ok', 'message': 'success', 'total': 0, 'data': None, 'error': ''}`).
5. **`put_to_parser`**:
   * Operational metric signaling data pushed to parsing pipeline.
6. **`404 gone`**:
   * Target items returning not found states.
7. **`sku not found`**:
   * Mapping errors indicating product absence.
8. **`isSuccess=true`**:
   * Operational flag validating a functional sub-routine passed successfully.

## Inferred

* **Custom Print & Traces**: The scrapers use rigorous standard-out formatting (like `[Часть X/Y] -> ✅ Отправлена успешно.`). This signifies that beyond basic status checks, we can deterministically parse logs for exact job completion percentages without relying exclusively on Crawlab statistics blocks.
* **Retries / Crash recovery**: The system prints warnings like `HOST_005 ошибка: парсер упал. Повторный запрос через 96 секунд`. This allows us to track stability and distinguish temporary failures heavily from terminal failures.

## Unresolved

* **Marker Uniqueness**: Are markers like `isSuccess=true` guaranteed uniquely per processed item, or can they duplicate inside loops during retries?
* **Crashes during Cancellation**: Will a log ALWAYS end with an explicit `{status: ok...}` struct if cancelled, or will it sometimes have SIGKILL fragments (`Killed`) like observed in `ID_744.log`? Classification rules need to prioritize the API `status=cancelled` over log trailers.
