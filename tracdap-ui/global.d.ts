/**
 * A global Typescript declaration file that allows images to be loaded and used as valid Typescript. A *.d.ts file is only
 * allowed to contain TypeScript code that doesn't generate any JavaScript code in the output.
 */

declare module "*.png" {
    const src: string
    export default src
}

/**
 * https://stackoverflow.com/questions/54121536/typescript-module-svg-has-no-exported-member-reactcomponent
 */
declare module '*.svg' {
    import React from "react";
    export const ReactComponent: React.FunctionComponent<React.SVGProps<SVGSVGElement>>;
    const src: string;
    export default src;
}

/**
 * https://github.com/Microsoft/TypeScript/issues/15146
 */
declare module '*.jpg';

// Global variables defined in webpack.config.ts
declare const TRAC_UI_VERSION: undefined | string;

// Global variables defined in webpack.config.ts
declare const TRAC_UI_EXPIRY: undefined | string;
