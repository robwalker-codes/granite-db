import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Run" }).waitFor();
  await page.evaluate(() => {
    window.__graniteMock?.reset();
  });
});

test("opens a database, refreshes schema after DDL, and manual refresh updates row counts", async ({ page }) => {
  await openDatabase(page);

  await expect(page.getByRole("button", { name: "Database /mock/sample.gdb" })).toBeVisible();
  await expect.poll(async () => {
    await page.evaluate(() => window.__graniteTest?.refreshMetadata());
    const metadata = await page.evaluate(() => window.__graniteTest?.getMetadata());
    return metadata?.tables.length ?? 0;
  }).toBe(0);

  await setEditorContent(page, "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
  await page.getByRole("button", { name: "Run" }).click();

  await expect.poll(async () => {
    return page.evaluate(() => {
      const metadata = window.__graniteTest?.getMetadata();
      return metadata?.tables.some((table) => table.name === "users") ?? false;
    });
  }).toBeTruthy();
  await expect(page.getByRole("button", { name: /users/ })).toBeVisible();

  await setEditorContent(page, "INSERT INTO users VALUES (1, 'Alice');");
  await page.getByRole("button", { name: "Run" }).click();

  const refreshButton = page.getByRole("button", { name: /Refresh/ });
  await refreshButton.click();
  await expect(refreshButton).toBeDisabled();
  await expect(refreshButton).toBeEnabled();

  await expect(page.getByRole("button", { name: /users/ })).toContainText("1 row(s)");
  await expect.poll(async () => {
    return page.evaluate(() => {
      const metadata = window.__graniteTest?.getMetadata();
      const table = metadata?.tables.find((item) => item.name === "users");
      return table?.rowCount ?? 0;
    });
  }).toBe(1);
});

test("toggles dark mode and updates the document theme", async ({ page }) => {
  const darkToggle = page.getByRole("button", { name: "Dark Theme" });
  await darkToggle.click();

  await expect(page.getByRole("button", { name: "Light Theme" })).toBeVisible();
  await expect.poll(async () => {
    return page.evaluate(() => document.documentElement.getAttribute("data-theme"));
  }).toBe("dark");
  await expect.poll(async () => {
    return page.evaluate(() => document.documentElement.classList.contains("dark"));
  }).toBeTruthy();

  const lightToggle = page.getByRole("button", { name: "Light Theme" });
  await lightToggle.click();

  await expect.poll(async () => {
    return page.evaluate(() => document.documentElement.classList.contains("dark"));
  }).toBeFalsy();
  await expect.poll(async () => {
    return page.evaluate(() => document.documentElement.getAttribute("data-theme"));
  }).toBe("light");
});

test("handles engine errors without blanking the UI", async ({ page }) => {
  await page.evaluate(() => {
    window.__graniteMock?.failNext("open_db", "Simulated failure");
  });

  await openDatabase(page);

  await expect(page.getByTitle("Simulated failure")).toBeVisible();
  await expect(page.getByRole("button", { name: "Run" })).toBeVisible();

  await openDatabase(page);

  await expect(page.getByRole("button", { name: "Database /mock/sample.gdb" })).toBeVisible();
});

async function openDatabase(page: import("@playwright/test").Page): Promise<void> {
  const databaseButton = page.getByRole("button", { name: /Database/ });
  await databaseButton.click();
  await page.getByRole("menuitem", { name: "File → Open…" }).click();
}

async function setEditorContent(page: import("@playwright/test").Page, value: string): Promise<void> {
  await page.evaluate((sql) => {
    window.__graniteTest?.setEditor(sql);
  }, value);
}

declare global {
  interface Window {
    __graniteMock?: {
      failNext(command: string, errorMessage: string): void;
      reset(): void;
    };
    __graniteTest?: {
      setEditor(value: string): void;
      refreshMetadata(): Promise<boolean>;
      getMetadata(): { tables: { name: string; rowCount: number }[] } | null;
    };
    monaco?: {
      editor?: {
        getTheme(): string;
      };
    };
  }
}

export {};
