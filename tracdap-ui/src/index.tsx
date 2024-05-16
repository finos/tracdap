import React from 'react';
import App from './App';
import {createRoot} from 'react-dom/client';
import {General} from "./config/config_general";
import {Provider} from 'react-redux'
import {store} from './storeController'

// This is a plugin that should only run in development that adds log messages when components are re-rendering without
// a change in props
if (process.env.NODE_ENV === 'development_') {
    const whyDidYouRender = require('@welldone-software/why-did-you-render');
    whyDidYouRender(React, {
        trackAllPureComponents: true,
        logOnDifferentValues: true,
        include: [/^ConnectFunction$/]
    });
}

// If the user hits the root URL, put app/ into the browser URL so the router knows what to serve
// This doesn't trigger an HTTP request, which means index.html will still work even if no rewrite rules are enabled
if (!window.location.pathname.includes(General.publicPath)) {

    window.history.pushState({}, "", "." + General.publicPath)
}

// Determine the router base path by looking at the URL and looking for the public path segment
// Normally this is /app/ so anything up to this will be the router base path
// E.g. for /my_deployment/app/page, /my_deployment/app/ will be detected as the router base path
const appPathIndex = window.location.pathname.indexOf(General.publicPath);
General.publicPath = appPathIndex > 0
    ? window.location.pathname.substring(0, appPathIndex + General.publicPath.length)
    : General.publicPath


// A reference to the application in the DOM
const container = document.getElementById('root');

// createRoot(container!) if you use TypeScript
const root = createRoot(container!);

root.render(
   //<React.StrictMode>
        <Provider store={store}>
            <App/>
        </Provider>
   //</React.StrictMode>
);