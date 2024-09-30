# TRAC UI

*A general user interface for TRAC d.a.p., a next-generation data and analytics platform for use in highly regulated environments*

## Development Status

[![Compliance](https://github.com/Accenture/trac-ui/actions/workflows/compliance.yml/badge.svg)](https://github.com/Accenture/trac-ui/actions/workflows/compliance.yml)
[![Build](https://github.com/Accenture/trac-ui/actions/workflows/build.yml/badge.svg)](https://github.com/Accenture/trac-ui/actions/workflows/build.yml)

# Set up for client

The demo user interface uses the TRAC d.a.p. logo and contains metadata that refers to the name and content of the application. These items need to be reviewed when deploying this appliation for a
client, specifically:

./public folder

favicon.ico 2022_tracdap_icon_purple_192.png 2022_tracdap_icon_purple_512.png index.html manifest.json

This contains the name of the application and it's content and may need updating. It also references the 2022_tracdap_icon_purple_192.png file so if the name of that image is changed then this file
needs to be updated.

# Testing

Testing is configured using [Jest](https://jestjs.io/). As part of the roadmap to version 1.0.0 the plan is to write tests for all utility functions and functions used by the component library (
src/react/components). It excludes testing of the rendering of the component library and the scenes. The test suites are in /tests and the jest config is ./jest.config.js.

# Typescript

[ts-loader](https://github.com/TypeStrong/ts-loader) is used by Webpack to load and transpile the Typescript code ahead of Babel with @babel/preset-typescript. This is so Typescript errors will
prevent the project being built and deployed. With Babel, Typescript errors would be highlighted in an IDE, but
it [allows projects with these errors to be built](https://github.com/microsoft/TypeScript-Babel-Starter/issues/33). The intention is pretty clear, if you have a Typescript bug then don't commit
without fixing it. The Typescript config file is ./tsconfig.json,

# Getting Started with TRAC UI

This project uses [React](https://reactjs.org/) as the Javascript framework, with [Redux](https://redux.js.org/) for state management and [React Bootstrap](react-bootstrap.github.io) as the core front
end framework. It is written in [Typescript](https://www.typescriptlang.org/).

## Available Scripts

The available script are listed in package.json. In the project directory, you can run:

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in your browser.

The page will reload when you make changes.\
You may also see any lint errors in the console.

### `npm test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `npm run build`

Builds the app for production to the `dist` folder.\
It correctly bundles the application in production mode and optimizes the build for the best performance.

### Code Splitting

This section has moved here: [https://facebook.github.io/create-react-app/docs/code-splitting](https://facebook.github.io/create-react-app/docs/code-splitting)

### Analyzing the Bundle Size

This section has moved here: [https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size](https://facebook.github.io/create-react-app/docs/analyzing-the-bundle-size)

### Making a Progressive Web AppTsx

This section has moved here: [https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app](https://facebook.github.io/create-react-app/docs/making-a-progressive-web-app)

### Advanced Configuration

This section has moved here: [https://facebook.github.io/create-react-app/docs/advanced-configuration](https://facebook.github.io/create-react-app/docs/advanced-configuration)

### Deployment

This section has moved here: [https://facebook.github.io/create-react-app/docs/deployment](https://facebook.github.io/create-react-app/docs/deployment)

### `npm run build` fails to minify

This section has moved
here: [https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify](https://facebook.github.io/create-react-app/docs/troubleshooting#npm-run-build-fails-to-minify)
