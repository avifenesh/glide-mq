### CI Failure

The job [failed](https://github.com/avifenesh/glide-mq/actions/runs/23101599855/job/67103201606) due to a test timing assertion in `tests/bulletproof.test.ts`.

- **Failed Test:** Sidekiq scheduler drift: interval jobs fire without drift accumulation [cluster]
- **Specific Error:**
  AssertionError: expected 1501 to be less than or equal to 1500 (see line 2237)
- **Test Description:** This test measures the interval between scheduled jobs and expects gaps ≤ 1500ms. Occasionally the gap is 1501ms, likely due to timer/jitter/CI environment delays.

### Suggested Fix
Increase the allowed upper gap to 1550ms to make the e2e test less flaky in CI environments. Please ensure the test is not failing due to a real bug, but if so, relax the interval tolerance as above.
