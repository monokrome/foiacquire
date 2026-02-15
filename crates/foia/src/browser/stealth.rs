//! Stealth evasion JavaScript to inject into pages.
//! Based on puppeteer-extra-plugin-stealth techniques.

pub const STEALTH_SCRIPTS: &[&str] = &[
    // Remove webdriver property
    r#"
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    "#,
    // Fix chrome object
    r#"
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    "#,
    // Fix permissions
    r#"
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
    );
    "#,
    // Fix plugins (make it look like regular Chrome)
    r#"
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
            { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
        ],
        configurable: true
    });
    "#,
    // Fix languages
    r#"
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    "#,
    // Fix platform (if on Linux, keep it; don't pretend to be Windows)
    r#"
    if (!navigator.platform.includes('Win')) {
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Linux x86_64',
            configurable: true
        });
    }
    "#,
    // Remove automation-related properties
    r#"
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    "#,
    // Fix WebGL vendor/renderer (common detection vector)
    r#"
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.call(this, parameter);
    };
    "#,
    // Fix hairline feature detection
    r#"
    Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
        get: function() {
            if (this.id === 'modernizr') return 1;
            return this.getBoundingClientRect().height;
        }
    });
    "#,
];
