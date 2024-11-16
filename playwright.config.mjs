import {defineConfig} from '@playwright/test';
import os from "os";

export default defineConfig({
    testDir: './tests',
    testMatch: '*.mjs',
    timeout: 30000,
    retries: 0,
    fullyParallel: true,
    expect: {
        timeout: 20000,
    },
    // https://youtrack.jetbrains.com/issue/AQUA-990/Playwright-test-results-output-is-duplicated?_gl=1*53npa7*_gcl_au*MjEzOTM1NzYwMS4xNzI3NzM4MTEx*_ga*MjEyNDI4NjUwOS4xNzA3MzU4MTA3*_ga_9J976DJZ68*MTcyODU4OTYyNi41LjAuMTcyODU4OTYyNi42MC4wLjA.
    reporter: 'null',
    workers: Math.max(os.cpus().length - 2, 1), // Number of parallel workers
});
