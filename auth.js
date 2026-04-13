/* auth.js — Dreck Suite shared authentication */
(function() {
    const CREDENTIALS = {
        'admin':  '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918',
        'justin': 'e6b5f42d6a37e1e44f2497de49d9c823d12f246b1a498e5e1089407e67e1be90',
        'matt':   'c0935fcb01df32945e26e2d8a3d5e86d3bfccfb72379f188641b98e08b5e543c',
        'max':    '76ff93bfb80e618bba09efb1a2bae9fb3391a63d27e3b0d459c3af4d7d10c8f0',
        'thomas': '3ca0cb5aee079d107b9ef6e55b89aa0e56c3d1be8b2a4e0f618847e0df893e05',
        'aaron':  '5a07cb2d7d43e0278a10e1f1fb3c2cc2dd059cc11b3b18e10d1f4e2a6f3e8c31'
    };

    async function sha256(message) {
        const msgBuffer = new TextEncoder().encode(message);
        const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    function isAuthenticated() {
        const session = localStorage.getItem('dreck_session');
        if (!session) return false;
        try {
            const data = JSON.parse(session);
            return data.expires > Date.now();
        } catch { return false; }
    }

    function getUser() {
        const session = localStorage.getItem('dreck_session');
        if (!session) return null;
        try {
            const data = JSON.parse(session);
            if (data.expires > Date.now()) return data.user;
        } catch {}
        return null;
    }

    function showLogin() {
        const overlay = document.createElement('div');
        overlay.id = 'auth-overlay';
        overlay.innerHTML = `
            <style>
                #auth-overlay {
                    position: fixed; inset: 0; z-index: 99999;
                    background: #0d0d14;
                    display: flex; align-items: center; justify-content: center;
                    font-family: 'Sora', sans-serif;
                }
                .auth-box {
                    background: #13131f; border: 1px solid #2a2a3a;
                    border-radius: 12px; padding: 40px; width: 340px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
                }
                .auth-box h2 {
                    font-family: 'Cormorant Garamond', serif;
                    color: #d4aa4f; margin: 0 0 8px 0; font-size: 28px;
                    font-weight: 600;
                }
                .auth-box .subtitle {
                    color: #6a6a8a; font-size: 13px; margin-bottom: 28px;
                }
                .auth-box input {
                    width: 100%; box-sizing: border-box;
                    background: #0d0d14; border: 1px solid #2a2a3a;
                    color: #e0e0e8; padding: 12px 14px; border-radius: 8px;
                    font-size: 14px; margin-bottom: 12px;
                    font-family: 'JetBrains Mono', monospace;
                }
                .auth-box input:focus {
                    outline: none; border-color: #d4aa4f;
                }
                .auth-box button {
                    width: 100%; padding: 12px;
                    background: #d4aa4f; color: #0d0d14;
                    border: none; border-radius: 8px; cursor: pointer;
                    font-weight: 600; font-size: 14px; margin-top: 4px;
                    font-family: 'Sora', sans-serif;
                    transition: background 0.2s;
                }
                .auth-box button:hover { background: #e0bb6a; }
                .auth-error {
                    color: #ff6b6b; font-size: 13px; margin-top: 12px;
                    display: none; text-align: center;
                }
            </style>
            <div class="auth-box">
                <h2>CRE Tracker</h2>
                <div class="subtitle">BusinessDen Internal Tool</div>
                <input type="text" id="auth-user" placeholder="Username" autocomplete="username">
                <input type="password" id="auth-pass" placeholder="Password" autocomplete="current-password">
                <button id="auth-submit">Sign In</button>
                <div class="auth-error" id="auth-error">Invalid credentials</div>
            </div>
        `;
        document.body.appendChild(overlay);

        const submit = async () => {
            const user = document.getElementById('auth-user').value.toLowerCase().trim();
            const pass = document.getElementById('auth-pass').value;
            const hash = await sha256(pass);
            if (CREDENTIALS[user] && CREDENTIALS[user] === hash) {
                localStorage.setItem('dreck_session', JSON.stringify({
                    user, expires: Date.now() + 30 * 24 * 60 * 60 * 1000
                }));
                overlay.remove();
                if (window.onAuthSuccess) window.onAuthSuccess();
            } else {
                document.getElementById('auth-error').style.display = 'block';
            }
        };
        document.getElementById('auth-submit').addEventListener('click', submit);
        document.getElementById('auth-pass').addEventListener('keydown', e => {
            if (e.key === 'Enter') submit();
        });
        document.getElementById('auth-user').focus();
    }

    window.DreckAuth = { isAuthenticated, getUser, showLogin };

    document.addEventListener('DOMContentLoaded', () => {
        if (!isAuthenticated()) showLogin();
        else if (window.onAuthSuccess) window.onAuthSuccess();
    });
})();
