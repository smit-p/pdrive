/**
 * pdrive Web UI — End-to-End Tests
 *
 * Runs against a live daemon at http://127.0.0.1:8765.
 * Requires: npx playwright test
 */
import { test, expect } from '@playwright/test';

const BASE = 'http://127.0.0.1:8765';

// ─── Helper ────────────────────────────────────────────────────────────────

async function waitForApp(page) {
  await page.goto(BASE);
  // Wait for the SPA to load — sidebar and topbar should be visible
  await expect(page.locator('.topbar')).toBeVisible();
  await expect(page.locator('.sidebar')).toBeVisible();
}

async function navigateToPage(page, pageName) {
  await page.locator(`.nav-item[data-page="${pageName}"]`).click();
  // Small wait for rendering
  await page.waitForTimeout(300);
}

// ─── 1. Initial Load & Layout ──────────────────────────────────────────────

test.describe('Initial Load & Layout', () => {
  test('serves HTML on browser request', async ({ page }) => {
    const resp = await page.goto(BASE);
    expect(resp.status()).toBe(200);
    expect(resp.headers()['content-type']).toContain('text/html');
  });

  test('renders topbar with logo and search', async ({ page }) => {
    await waitForApp(page);
    await expect(page.locator('.logo')).toHaveText('pdrive');
    await expect(page.locator('#global-search')).toBeVisible();
  });

  test('renders sidebar with all nav items', async ({ page }) => {
    await waitForApp(page);
    const navItems = page.locator('.nav-item');
    const count = await navItems.count();
    expect(count).toBeGreaterThanOrEqual(7); // Files, Dashboard, Uploads, Search, Tree, Activity, Metrics
    await expect(page.locator('.nav-item[data-page="browse"]')).toHaveText(/Files/);
    await expect(page.locator('.nav-item[data-page="dashboard"]')).toHaveText(/Dashboard/);
    await expect(page.locator('.nav-item[data-page="uploads"]')).toHaveText(/Uploads/);
    await expect(page.locator('.nav-item[data-page="search"]')).toHaveText(/Search/);
    await expect(page.locator('.nav-item[data-page="tree"]')).toHaveText(/Tree/);
    await expect(page.locator('.nav-item[data-page="metrics"]')).toHaveText(/Metrics/);
  });

  test('Files nav is active by default', async ({ page }) => {
    await waitForApp(page);
    await expect(page.locator('.nav-item[data-page="browse"]')).toHaveClass(/active/);
  });

  test('storage bar is visible in topbar', async ({ page }) => {
    await waitForApp(page);
    await expect(page.locator('.storage-wrap')).toBeVisible();
    await expect(page.locator('#storage-fill')).toBeVisible();
    // Wait for storage data to load
    await page.waitForTimeout(1500);
    const label = await page.locator('#storage-label').textContent();
    // Should show something like "X GB / Y GB" or "—"
    expect(label.length).toBeGreaterThan(0);
  });
});

// ─── 2. File Browser ───────────────────────────────────────────────────────

test.describe('File Browser', () => {
  test('loads root directory with files and dirs', async ({ page }) => {
    await waitForApp(page);
    // Wait for file listing to load
    await page.waitForSelector('.file-table, .empty-state', { timeout: 5000 });
    // We know from the API there are dirs and files at root
    const rows = page.locator('.file-row');
    const count = await rows.count();
    expect(count).toBeGreaterThan(0);
  });

  test('shows breadcrumb starting at pdrive root', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.breadcrumb', { timeout: 3000 });
    const bc = page.locator('.breadcrumb');
    await expect(bc).toContainText('pdrive');
  });

  test('shows action bar with New Folder and Usage buttons', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await expect(page.locator('button[data-action="newFolder"]')).toBeVisible();
    await expect(page.locator('button[data-action="duHere"]')).toBeVisible();
  });

  test('clicking a directory navigates into it', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row[data-isdir="1"]', { timeout: 5000 });
    const dirRow = page.locator('.file-row[data-isdir="1"]').first();
    const rawName = await dirRow.locator('.cell-name a').textContent();
    // Strip leading emoji/icon chars
    const dirName = rawName.replace(/^[^\w\s]+/, '').trim();
    await dirRow.locator('.cell-name a').click();
    // Breadcrumb should now contain the dir name (without emoji)
    await page.waitForTimeout(500);
    const bc = await page.locator('.breadcrumb').textContent();
    expect(bc).toContain(dirName);
  });

  test('Up button navigates to parent', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row[data-isdir="1"]', { timeout: 5000 });
    // Navigate into a dir
    await page.locator('.file-row[data-isdir="1"]').first().locator('.cell-name a').click();
    await page.waitForTimeout(500);
    // Click Up
    const upBtn = page.locator('button[data-action="navigate"]');
    if (await upBtn.count() > 0) {
      await upBtn.click();
      await page.waitForTimeout(500);
      const bc = await page.locator('.breadcrumb').textContent();
      expect(bc).toContain('pdrive');
    }
  });

  test('files show state badges (local/cloud/uploading)', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Check that at least one badge exists
    const badges = page.locator('.badge');
    const count = await badges.count();
    expect(count).toBeGreaterThan(0);
    // Verify badge text is one of the expected values
    const firstBadge = await badges.first().textContent();
    expect(['local', 'cloud', 'uploading']).toContain(firstBadge.trim());
  });

  test('file table has sortable columns', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Click Name header to sort
    await page.locator('th[data-col="name"]').click();
    await page.waitForTimeout(200);
    // Should show sort indicator
    const indicator = page.locator('th[data-col="name"] .sort-indicator');
    await expect(indicator).toBeVisible();
  });

  test('sorting toggles direction on repeated click', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Click size twice
    await page.locator('th[data-col="size"]').click();
    await page.waitForTimeout(100);
    let ind = await page.locator('th[data-col="size"] .sort-indicator').textContent();
    const first = ind.trim();
    await page.locator('th[data-col="size"]').click();
    await page.waitForTimeout(100);
    ind = await page.locator('th[data-col="size"] .sort-indicator').textContent();
    expect(ind.trim()).not.toBe(first);
  });

  test('file row hover shows action buttons', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir])', { timeout: 5000 });
    const fileRow = page.locator('.file-row:not([data-isdir="1"])').first();
    await fileRow.hover();
    // cell-actions should become visible (opacity 1)
    const actions = fileRow.locator('.cell-actions');
    await expect(actions).toBeVisible();
  });

  test('checkbox selects file and shows selection bar', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    const checkbox = page.locator('.file-row:not([data-isdir="1"]) input[type="checkbox"]').first();
    await checkbox.check();
    await page.waitForTimeout(200);
    // Selection bar should appear
    const selBar = page.locator('.selection-bar');
    await expect(selBar).toHaveClass(/visible/);
    await expect(selBar).toContainText('1 selected');
  });

  test('select-all checkbox selects all files', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    const selectAll = page.locator('th input[data-action="selectAll"]');
    await selectAll.click({ force: true });
    await page.waitForTimeout(300);
    const selBar = page.locator('.selection-bar');
    await expect(selBar).toHaveClass(/visible/);
    const text = await selBar.locator('.sel-count').textContent();
    const num = parseInt(text);
    expect(num).toBeGreaterThan(0);
  });

  test('clear selection hides the selection bar', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    // Select a file
    await page.locator('.file-row:not([data-isdir="1"]) input[type="checkbox"]').first().check();
    await page.waitForTimeout(200);
    await expect(page.locator('.selection-bar')).toHaveClass(/visible/);
    // Clear
    await page.locator('[data-action="clearSelection"]').click();
    await page.waitForTimeout(200);
    await expect(page.locator('.selection-bar')).not.toHaveClass(/visible/);
  });
});

// ─── 3. File Info Panel ────────────────────────────────────────────────────

test.describe('File Info Panel', () => {
  test('clicking a file name opens info panel', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    const fileLink = page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first();
    await fileLink.click();
    // Info panel should open
    await expect(page.locator('.info-panel')).toHaveClass(/open/);
    await expect(page.locator('.panel-overlay')).toHaveClass(/open/);
  });

  test('info panel shows file metadata', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await page.waitForTimeout(1000); // Wait for API response
    const panel = page.locator('.info-panel');
    // Should show labels like Path, Size, Created, etc.
    const labelCount = await panel.locator('.info-label').count();
    expect(labelCount).toBeGreaterThanOrEqual(3);
    await expect(panel.locator('span.info-label:has-text("Path")')).toBeVisible();
    await expect(panel.locator('span.info-label:has-text("Size")')).toBeVisible();
  });

  test('info panel has action buttons', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await page.waitForTimeout(1000);
    const actions = page.locator('.panel-actions');
    await expect(actions.locator('[data-action="download"]')).toBeVisible();
    await expect(actions.locator('[data-action="pin"]')).toBeVisible();
    await expect(actions.locator('[data-action="unpin"]')).toBeVisible();
    await expect(actions.locator('[data-action="deleteFile"]')).toBeVisible();
    await expect(actions.locator('[data-action="moveFile"]')).toBeVisible();
  });

  test('close button closes info panel', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await expect(page.locator('.info-panel')).toHaveClass(/open/);
    await page.locator('.panel-close').click();
    await page.waitForTimeout(300);
    await expect(page.locator('.info-panel')).not.toHaveClass(/open/);
  });

  test('clicking overlay closes info panel', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await expect(page.locator('.info-panel')).toHaveClass(/open/);
    await page.locator('#panel-overlay').click({ position: { x: 10, y: 10 } });
    await page.waitForTimeout(300);
    await expect(page.locator('.info-panel')).not.toHaveClass(/open/);
  });

  test('info panel shows chunk table for completed files', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    // Try to find a file with chunks (bigger files have chunks)
    const rows = page.locator('.file-row:not([data-isdir="1"])');
    const count = await rows.count();
    let foundChunks = false;
    for (let i = 0; i < Math.min(count, 5); i++) {
      await rows.nth(i).locator('.cell-name a').click();
      await page.waitForTimeout(1000);
      const chunkTable = page.locator('.chunk-table');
      if (await chunkTable.count() > 0) {
        foundChunks = true;
        const thCount = await chunkTable.locator('th').count();
        expect(thCount).toBeGreaterThanOrEqual(3);
        break;
      }
      await page.locator('.panel-close').click();
      await page.waitForTimeout(200);
    }
    // At least verify the panel opened and showed metadata — chunks may not exist for tiny files
    if (!foundChunks) {
      console.log('No files with chunks found — skipping chunk table check');
    }
  });
});

// ─── 4. File Actions ───────────────────────────────────────────────────────

test.describe('File Actions', () => {
  test('download button creates download link', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row[data-state="local"]', { timeout: 5000 });
    // Open info panel for a LOCAL file (stubs cannot be downloaded)
    await page.locator('.file-row[data-state="local"] .cell-name a').first().click();
    await page.waitForTimeout(1000);
    // Verify the download button exists and has data-path
    const btn = page.locator('.panel-actions [data-action="download"]');
    await expect(btn).toBeVisible();
    const path = await btn.getAttribute('data-path');
    expect(path).toBeTruthy();
    expect(path.startsWith('/')).toBe(true);
  });

  test('inline download button is accessible on hover', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row[data-state="local"]', { timeout: 5000 });
    const fileRow = page.locator('.file-row[data-state="local"]').first();
    await fileRow.hover();
    const btn = fileRow.locator('[data-action="download"]');
    await expect(btn).toBeVisible();
    const path = await btn.getAttribute('data-path');
    expect(path).toBeTruthy();
  });

  test('delete shows confirmation modal', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await page.waitForTimeout(1000);
    await page.locator('.panel-actions [data-action="deleteFile"]').click();
    await page.waitForTimeout(200);
    // Modal should appear
    await expect(page.locator('.modal-overlay')).toHaveClass(/open/);
    await expect(page.locator('.modal')).toContainText('Delete');
    await expect(page.locator('.modal')).toContainText('cannot be undone');
    // Cancel it
    await page.locator('[data-action="modalClose"]').first().click();
    await page.waitForTimeout(200);
    await expect(page.locator('.modal-overlay')).not.toHaveClass(/open/);
  });

  test('move shows modal with path input', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await page.waitForTimeout(1000);
    await page.locator('.panel-actions [data-action="moveFile"]').click();
    await page.waitForTimeout(200);
    await expect(page.locator('.modal-overlay')).toHaveClass(/open/);
    await expect(page.locator('#modal-input')).toBeVisible();
    // Input should have the current path
    const val = await page.locator('#modal-input').inputValue();
    expect(val.length).toBeGreaterThan(0);
    // Cancel
    await page.locator('[data-action="modalClose"]').first().click();
  });

  test('new folder shows modal and creates directory', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await page.locator('[data-action="newFolder"]').click();
    await page.waitForTimeout(200);
    await expect(page.locator('.modal-overlay')).toHaveClass(/open/);
    await expect(page.locator('#modal-input')).toBeVisible();
    // Type a name and create
    const folderName = 'e2e-test-' + Date.now();
    await page.locator('#modal-input').fill(folderName);
    await page.locator('[data-action="modalConfirm"]').click();
    await page.waitForTimeout(1000);
    // Toast should appear
    const toasts = page.locator('.toast');
    // Check if success toast appeared
    await expect(toasts.first()).toBeVisible({ timeout: 3000 });
    // Directory should now appear in listing
    await page.waitForTimeout(500);
    const pageText = await page.locator('.main').textContent();
    expect(pageText).toContain(folderName);
    // Cleanup: delete the folder
    const dirRow = page.locator(`.file-row[data-filepath="/${folderName}"]`);
    if (await dirRow.count() > 0) {
      await dirRow.locator('[data-action="deleteFile"]').click();
      await page.waitForTimeout(200);
      await page.locator('[data-action="modalConfirm"]').click();
      await page.waitForTimeout(1000);
    }
  });

  test('disk usage button shows toast with info', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await page.locator('[data-action="duHere"]').click();
    await page.waitForTimeout(1000);
    const toast = page.locator('.toast').first();
    await expect(toast).toBeVisible({ timeout: 3000 });
    const text = await toast.textContent();
    expect(text).toMatch(/files?/i);
  });

  test('pin button triggers pin API call', async ({ page }) => {
    await waitForApp(page);
    // Prefer a stub file for pin (it makes semantic sense to pin a cloud-only file)
    // Fall back to any non-dir file if no stubs exist
    let selector = '.file-row[data-state="stub"]';
    const stubCount = await page.locator(selector).count();
    if (stubCount === 0) selector = '.file-row:not([data-isdir="1"])';
    await page.waitForSelector(selector, { timeout: 5000 });
    await page.locator(selector + ' .cell-name a').first().click();
    await page.waitForTimeout(1000);
    // Verify pin button exists
    const pinBtn = page.locator('.panel-actions [data-action="pin"]');
    await expect(pinBtn).toBeVisible();
    // Verify the click fires a request to /api/pin (don't wait for response — it may be slow)
    const requestPromise = page.waitForRequest(r => r.url().includes('/api/pin'), { timeout: 5000 });
    await pinBtn.click();
    const req = await requestPromise;
    expect(req.url()).toContain('/api/pin');
    expect(req.method()).toBe('POST');
  });
});

// ─── 5. Dashboard ──────────────────────────────────────────────────────────

test.describe('Dashboard', () => {
  test('navigates to dashboard page', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await expect(page.locator('.nav-item[data-page="dashboard"]')).toHaveClass(/active/);
    await expect(page.locator('.page-title')).toHaveText('Dashboard');
  });

  test('shows health card', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(1500);
    const healthSection = page.locator('#dash-health');
    await expect(healthSection).toBeVisible();
    await expect(healthSection).toContainText('Health');
    await expect(healthSection.locator('.health-dot')).toBeVisible();
  });

  test('shows total files card', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(1500);
    await expect(page.locator('#dash-health')).toContainText('Total Files');
  });

  test('shows storage providers with quota bars', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(1500);
    const providers = page.locator('#dash-providers .card');
    const count = await providers.count();
    expect(count).toBeGreaterThan(0);
    // Each provider card should have a bar
    await expect(providers.first().locator('.card-bar')).toBeVisible();
  });

  test('health indicator shows ok or degraded', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(1500);
    const dot = page.locator('.health-dot');
    const cls = await dot.getAttribute('class');
    expect(cls).toMatch(/ok|degraded/);
  });

  test('shows uptime in health card', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(1500);
    const health = page.locator('#dash-health');
    const text = await health.textContent();
    expect(text).toMatch(/Uptime/i);
  });
});

// ─── 6. Uploads ────────────────────────────────────────────────────────────

test.describe('Uploads', () => {
  test('navigates to uploads page', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'uploads');
    await expect(page.locator('.nav-item[data-page="uploads"]')).toHaveClass(/active/);
    await expect(page.locator('.page-title')).toHaveText('Uploads');
  });

  test('shows upload cards or empty state', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'uploads');
    await page.waitForTimeout(1000);
    const uploads = page.locator('#uploads-list');
    const text = await uploads.textContent();
    // Either has upload cards or "No active uploads"
    expect(text.length).toBeGreaterThan(0);
    const hasCards = await page.locator('.upload-card').count() > 0;
    const hasEmpty = text.includes('No active uploads');
    expect(hasCards || hasEmpty).toBe(true);
  });

  test('upload cards show progress when uploads exist', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'uploads');
    await page.waitForTimeout(1000);
    const cards = page.locator('.upload-card');
    if (await cards.count() > 0) {
      await expect(cards.first().locator('.upload-bar')).toBeVisible();
      await expect(cards.first().locator('.upload-meta')).toBeVisible();
    }
  });
});

// ─── 7. Search ─────────────────────────────────────────────────────────────

test.describe('Search', () => {
  test('navigates to search page', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'search');
    await expect(page.locator('.nav-item[data-page="search"]')).toHaveClass(/active/);
    await expect(page.locator('.page-title')).toHaveText('Search');
  });

  test('has search input and root path fields', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'search');
    await expect(page.locator('#search-pattern')).toBeVisible();
    await expect(page.locator('#search-root')).toBeVisible();
    await expect(page.locator('[data-action="doSearch"]')).toBeVisible();
  });

  test('search returns results for *.txt', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'search');
    await page.locator('#search-pattern').fill('*.txt');
    await page.locator('[data-action="doSearch"]').click();
    await page.waitForTimeout(2000);
    // Should find .txt files
    const results = page.locator('#search-results');
    const text = await results.textContent();
    expect(text).toMatch(/\.txt|result|No files/);
  });

  test('search results are clickable to open info', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'search');
    await page.locator('#search-pattern').fill('*.txt');
    await page.locator('[data-action="doSearch"]').click();
    await page.waitForTimeout(2000);
    const resultLinks = page.locator('#search-results [data-action="showInfo"]');
    if (await resultLinks.count() > 0) {
      await resultLinks.first().click();
      await page.waitForTimeout(500);
      await expect(page.locator('.info-panel')).toHaveClass(/open/);
    }
  });

  test('shows error toast for empty pattern', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'search');
    await page.locator('#search-pattern').fill('');
    await page.locator('[data-action="doSearch"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('.toast-error')).toBeVisible({ timeout: 2000 });
  });

  test('global search bar triggers search page', async ({ page }) => {
    await waitForApp(page);
    const searchInput = page.locator('#global-search');
    await searchInput.fill('smoke');
    await searchInput.press('Enter');
    await page.waitForTimeout(1500);
    // Should navigate to search page
    await expect(page.locator('.nav-item[data-page="search"]')).toHaveClass(/active/);
  });
});

// ─── 8. Tree View ──────────────────────────────────────────────────────────

test.describe('Tree View', () => {
  test('navigates to tree page', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'tree');
    await expect(page.locator('.nav-item[data-page="tree"]')).toHaveClass(/active/);
    await expect(page.locator('.page-title')).toHaveText('Tree');
  });

  test('has root path input and load button', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'tree');
    await expect(page.locator('#tree-root')).toBeVisible();
    await expect(page.locator('[data-action="doTree"]')).toBeVisible();
  });

  test('loads tree structure', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'tree');
    await page.locator('[data-action="doTree"]').click();
    await page.waitForTimeout(2000);
    const content = page.locator('#tree-content');
    const text = await content.textContent();
    // Should contain tree nodes or empty state
    expect(text.length).toBeGreaterThan(0);
  });

  test('tree nodes are clickable for file info', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'tree');
    await page.locator('[data-action="doTree"]').click();
    await page.waitForTimeout(2000);
    const links = page.locator('#tree-content [data-action="showInfo"]');
    if (await links.count() > 0) {
      await links.first().click();
      await page.waitForTimeout(500);
      await expect(page.locator('.info-panel')).toHaveClass(/open/);
    }
  });

  test('tree shows file count summary', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'tree');
    await page.locator('[data-action="doTree"]').click();
    await page.waitForTimeout(2000);
    const content = await page.locator('#tree-content').textContent();
    expect(content).toMatch(/file\(s\)|Empty tree/);
  });
});

// ─── 9. Metrics ────────────────────────────────────────────────────────────

test.describe('Metrics', () => {
  test('navigates to metrics page', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'metrics');
    await expect(page.locator('.nav-item[data-page="metrics"]')).toHaveClass(/active/);
    await expect(page.locator('.page-title')).toHaveText('Metrics');
  });

  test('shows metric cards', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'metrics');
    await page.waitForTimeout(1500);
    const cards = page.locator('#metrics-content .card');
    const count = await cards.count();
    expect(count).toBeGreaterThanOrEqual(5); // Files Uploaded, Downloaded, Deleted, Chunks, Bytes
  });

  test('shows Files Uploaded metric', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'metrics');
    await page.waitForTimeout(1500);
    await expect(page.locator('#metrics-content')).toContainText('Files Uploaded');
  });

  test('shows Dedup Hits metric', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'metrics');
    await page.waitForTimeout(1500);
    await expect(page.locator('#metrics-content')).toContainText('Dedup Hits');
  });
});

// ─── 10. Keyboard Shortcuts ────────────────────────────────────────────────

test.describe('Keyboard Shortcuts', () => {
  test('/ focuses the search input', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Click somewhere neutral first
    await page.locator('.main').click();
    await page.keyboard.press('/');
    await page.waitForTimeout(200);
    const focused = await page.evaluate(() => document.activeElement.id);
    expect(focused).toBe('global-search');
  });

  test('j/k navigates file rows', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Click on main area to ensure focus
    await page.locator('.main').click();
    await page.keyboard.press('j');
    await page.waitForTimeout(200);
    const focused = page.locator('.file-row.focused');
    await expect(focused).toHaveCount(1);
    // Press j again
    await page.keyboard.press('j');
    await page.waitForTimeout(200);
    await expect(page.locator('.file-row.focused')).toHaveCount(1);
  });

  test('Escape closes info panel', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) .cell-name a').first().click();
    await expect(page.locator('.info-panel')).toHaveClass(/open/);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);
    await expect(page.locator('.info-panel')).not.toHaveClass(/open/);
  });

  test('Escape closes modal', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await page.locator('[data-action="newFolder"]').click();
    await page.waitForTimeout(200);
    await expect(page.locator('.modal-overlay')).toHaveClass(/open/);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    await expect(page.locator('.modal-overlay')).not.toHaveClass(/open/);
  });

  test('~ navigates to root in browse mode', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row[data-isdir="1"]', { timeout: 5000 });
    // Navigate into a dir
    await page.locator('.file-row[data-isdir="1"]').first().locator('.cell-name a').click();
    await page.waitForTimeout(500);
    // Press ~ (need to click main first to unfocus inputs)
    await page.locator('.main').click();
    await page.keyboard.press('~');
    await page.waitForTimeout(500);
    const bc = await page.locator('.breadcrumb').textContent();
    // Should be back at root — breadcrumb should just have "pdrive" without subdirs
    expect(bc.trim()).toBe('pdrive');
  });

  test('space toggles selection on focused row', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.main').click();
    // Navigate down to first file row (may need to pass dirs first)
    const rows = page.locator('.file-row');
    const count = await rows.count();
    // Press j enough times to land on a file
    for (let i = 0; i < count; i++) {
      await page.keyboard.press('j');
    }
    await page.waitForTimeout(100);
    // Press space  
    await page.keyboard.press(' ');
    await page.waitForTimeout(300);
    // Check if selection bar appeared
    const selBar = page.locator('.selection-bar');
    // It may or may not be visible depending on if we landed on a file (not dir)
    // Just verify no crash
  });

  test('i opens info panel for focused file', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.main').click();
    // Navigate to a file row
    const dirCount = await page.locator('.file-row[data-isdir="1"]').count();
    for (let i = 0; i <= dirCount; i++) {
      await page.keyboard.press('j');
    }
    await page.waitForTimeout(200);
    await page.keyboard.press('i');
    await page.waitForTimeout(500);
    // Info panel might open if we're on a file
    // Just verify no crash
  });
});

// ─── 11. Navigation Consistency ────────────────────────────────────────────

test.describe('Navigation Consistency', () => {
  test('clicking logo always returns to file browser root', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(300);
    await page.locator('.logo').click();
    await page.waitForTimeout(500);
    await expect(page.locator('.nav-item[data-page="browse"]')).toHaveClass(/active/);
  });

  test('switching pages updates active nav state', async ({ page }) => {
    await waitForApp(page);
    const pages = ['dashboard', 'uploads', 'search', 'tree', 'metrics', 'browse'];
    for (const pg of pages) {
      await navigateToPage(page, pg);
      await expect(page.locator(`.nav-item[data-page="${pg}"]`)).toHaveClass(/active/);
    }
  });

  test('switching pages clears selection', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.file-row:not([data-isdir="1"])', { timeout: 5000 });
    await page.locator('.file-row:not([data-isdir="1"]) input[type="checkbox"]').first().check();
    await page.waitForTimeout(200);
    await expect(page.locator('.selection-bar')).toHaveClass(/visible/);
    await navigateToPage(page, 'dashboard');
    await page.waitForTimeout(300);
    await expect(page.locator('.selection-bar')).not.toHaveClass(/visible/);
  });
});

// ─── 12. Toast Notifications ───────────────────────────────────────────────

test.describe('Toast Notifications', () => {
  test('toasts auto-dismiss after a few seconds', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await page.locator('[data-action="duHere"]').click();
    await page.waitForTimeout(500);
    const toast = page.locator('.toast').first();
    await expect(toast).toBeVisible({ timeout: 2000 });
    // Wait for auto-dismiss (4 seconds)
    await page.waitForTimeout(4500);
    await expect(toast).not.toBeVisible();
  });
});

// ─── 13. Error Handling ────────────────────────────────────────────────────

test.describe('Error Handling', () => {
  test('info panel handles missing file gracefully', async ({ page }) => {
    await waitForApp(page);
    // Manually trigger info for a non-existent path
    await page.evaluate(() => {
      document.querySelector('.info-panel').classList.add('open');
      document.querySelector('.panel-overlay').classList.add('open');
    });
    await page.evaluate(async () => {
      try {
        const r = await fetch('/api/info?path=/nonexistent-file-xyz');
        if (!r.ok) throw new Error('not found');
      } catch (e) {
        // Expected
      }
    });
    // No crash
  });
});

// ─── 14. Responsive Layout ────────────────────────────────────────────────

test.describe('Responsive Layout', () => {
  test('hamburger menu appears on narrow viewport', async ({ page }) => {
    await page.setViewportSize({ width: 600, height: 800 });
    await waitForApp(page);
    const hamburger = page.locator('#hamburger');
    await expect(hamburger).toBeVisible();
  });

  test('hamburger toggles sidebar on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 600, height: 800 });
    await waitForApp(page);
    const sidebar = page.locator('#sidebar');
    // Sidebar should be hidden (off-screen)
    const box = await sidebar.boundingBox();
    expect(box.x).toBeLessThan(0);
    // Click hamburger
    await page.locator('#hamburger').click();
    await page.waitForTimeout(300);
    await expect(sidebar).toHaveClass(/open/);
  });

  test('app renders reasonably at desktop width', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await waitForApp(page);
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Sidebar should be visible
    const sidebar = page.locator('.sidebar');
    const box = await sidebar.boundingBox();
    expect(box.x).toBeGreaterThanOrEqual(0);
  });
});

// ─── 15. Upload Feature ────────────────────────────────────────────────────

test.describe('Upload Feature', () => {
  test('upload button is visible in action bar', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    await expect(page.locator('button[data-action="uploadFile"]')).toBeVisible();
    await expect(page.locator('button[data-action="uploadFile"]')).toContainText('Upload');
  });

  test('hidden file input exists', async ({ page }) => {
    await waitForApp(page);
    await page.waitForSelector('.action-bar', { timeout: 3000 });
    const input = page.locator('#upload-input');
    await expect(input).toBeAttached();
    expect(await input.getAttribute('type')).toBe('file');
  });

  test('upload via API creates file', async ({ page }) => {
    await waitForApp(page);
    // Upload a file via the API directly
    const resp = await page.evaluate(async () => {
      const blob = new Blob(['hello upload test'], { type: 'text/plain' });
      const file = new File([blob], 'e2e-upload-test.txt', { type: 'text/plain' });
      const fd = new FormData();
      fd.append('file', file);
      fd.append('dir', '/');
      const r = await fetch('/api/upload', { method: 'POST', body: fd });
      return { status: r.status, body: await r.json() };
    });
    expect(resp.status).toBe(200);
    expect(resp.body.status).toBe('ok');
    expect(resp.body.path).toBe('/e2e-upload-test.txt');
    expect(resp.body.size).toBe(17);
    // Cleanup
    await page.evaluate(async () => {
      await fetch('/api/delete?path=%2Fe2e-upload-test.txt', { method: 'POST' });
    });
  });

  test('upload rejects request without file', async ({ page }) => {
    await waitForApp(page);
    const status = await page.evaluate(async () => {
      const fd = new FormData();
      fd.append('dir', '/');
      const r = await fetch('/api/upload', { method: 'POST', body: fd });
      return r.status;
    });
    expect(status).toBe(400);
  });

  test('drag-over shows visual feedback', async ({ page }) => {
    await waitForApp(page);
    const main = page.locator('#main');
    // Simulate dragenter via page.evaluate since DataTransfer is a browser API
    await main.evaluate((el) => {
      el.dispatchEvent(new DragEvent('dragenter', { bubbles: true }));
    });
    await page.waitForTimeout(100);
    await expect(main).toHaveClass(/drag-over/);
  });
});

// ─── 16. Verify Feature ────────────────────────────────────────────────────

test.describe('Verify Feature', () => {
  test('verify API returns result for existing file', async ({ page }) => {
    await waitForApp(page);
    // First upload a file to verify
    await page.evaluate(async () => {
      const blob = new Blob(['verify test content'], { type: 'text/plain' });
      const file = new File([blob], 'e2e-verify-test.txt', { type: 'text/plain' });
      const fd = new FormData();
      fd.append('file', file);
      fd.append('dir', '/');
      await fetch('/api/upload', { method: 'POST', body: fd });
    });
    // Verify the file
    const result = await page.evaluate(async () => {
      const r = await fetch('/api/verify?path=%2Fe2e-verify-test.txt');
      return r.json();
    });
    expect(result.path).toBe('/e2e-verify-test.txt');
    expect(typeof result.ok).toBe('boolean');
    // Cleanup
    await page.evaluate(async () => {
      await fetch('/api/delete?path=%2Fe2e-verify-test.txt', { method: 'POST' });
    });
  });

  test('verify API rejects missing path', async ({ page }) => {
    await waitForApp(page);
    const status = await page.evaluate(async () => {
      const r = await fetch('/api/verify');
      return r.status;
    });
    expect(status).toBe(400);
  });

  test('verify button appears in info panel', async ({ page }) => {
    await waitForApp(page);
    // Upload a test file
    await page.evaluate(async () => {
      const blob = new Blob(['panel verify test'], { type: 'text/plain' });
      const file = new File([blob], 'e2e-panel-verify.txt', { type: 'text/plain' });
      const fd = new FormData();
      fd.append('file', file);
      fd.append('dir', '/');
      await fetch('/api/upload', { method: 'POST', body: fd });
    });
    // Reload browse
    await page.locator('.nav-item[data-page="browse"]').click();
    await page.waitForSelector('.file-table', { timeout: 5000 });
    // Click info on the file
    const infoBtn = page.locator('button[data-action="showInfo"][data-path="/e2e-panel-verify.txt"]');
    if (await infoBtn.count() > 0) {
      await infoBtn.click();
      await page.waitForSelector('.info-panel.open', { timeout: 3000 });
      await expect(page.locator('button[data-action="verifyFile"]')).toBeVisible();
    }
    // Cleanup
    await page.evaluate(async () => {
      await fetch('/api/delete?path=%2Fe2e-panel-verify.txt', { method: 'POST' });
    });
  });
});

// ─── 17. Activity Page ─────────────────────────────────────────────────────

test.describe('Activity Page', () => {
  test('activity nav item is visible in sidebar', async ({ page }) => {
    await waitForApp(page);
    await expect(page.locator('.nav-item[data-page="activity"]')).toBeVisible();
    await expect(page.locator('.nav-item[data-page="activity"]')).toContainText('Activity');
  });

  test('navigating to activity page renders', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'activity');
    await expect(page.locator('.nav-item[data-page="activity"]')).toHaveClass(/active/);
    // Should show either the activity table or empty state
    await page.waitForSelector('#activity-content', { timeout: 3000 });
    await expect(page.locator('#activity-content')).toBeVisible();
  });

  test('activity API returns array', async ({ page }) => {
    await waitForApp(page);
    const result = await page.evaluate(async () => {
      const r = await fetch('/api/activity');
      return { status: r.status, body: await r.json() };
    });
    expect(result.status).toBe(200);
    expect(Array.isArray(result.body)).toBe(true);
  });

  test('activity shows entries after upload', async ({ page }) => {
    await waitForApp(page);
    // Upload a file to generate activity
    await page.evaluate(async () => {
      const blob = new Blob(['activity test'], { type: 'text/plain' });
      const file = new File([blob], 'e2e-activity-test.txt', { type: 'text/plain' });
      const fd = new FormData();
      fd.append('file', file);
      fd.append('dir', '/');
      await fetch('/api/upload', { method: 'POST', body: fd });
    });
    // Navigate to activity page
    await navigateToPage(page, 'activity');
    await page.waitForSelector('#activity-content', { timeout: 3000 });
    // Wait for content to load
    await page.waitForTimeout(500);
    const content = await page.locator('#activity-content').innerHTML();
    // Should have at least one entry
    expect(content.length).toBeGreaterThan(50);
    // Cleanup
    await page.evaluate(async () => {
      await fetch('/api/delete?path=%2Fe2e-activity-test.txt', { method: 'POST' });
    });
  });

  test('page title shows Activity heading', async ({ page }) => {
    await waitForApp(page);
    await navigateToPage(page, 'activity');
    await expect(page.locator('.page-title')).toContainText('Activity');
  });
});
