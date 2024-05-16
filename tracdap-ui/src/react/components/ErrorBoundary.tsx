/**
 * A component that is used to wrap other components to prevent errors from these child components from
 * causing the whole application to crash. When using this approach an error will result in the child
 * component being replaced with an error message. See https://reactjs.org/docs/error-boundaries.html
 *
 * @module ErrorBoundary
 * @category Component
 */

import {Alert} from "./Alert";
import React from "react";
import type {ErrorInfo} from "react";

/**
 * An interface for the props of the ErrorBoundary component.
 */
interface Props {
    children: React.ReactElement<any, any> | React.ReactElement<any, any>[];
}

/**
 * An interface for the state of the ErrorBoundary component.
 */
interface State {
    hasError: boolean;
}

export default class ErrorBoundary extends React.Component<Props, State> {

    constructor(props: Props) {

        super(props);
        this.state = {hasError: false};
    }

    /**
     * A function that gets called by the internals of React if an error happens. This is a render phase
     * lifecycle function that does not force sync re-rendering. That means it sets state before the render.
     *
     * @param error - The error that occurred.
     * @returns {{hasError: boolean}}
     */
    static getDerivedStateFromError(error: Error) {

        // Update state so the next render will show the fallback UI.
        return {hasError: true};
    }

    /**
     * A function that gets called by the internals of React if an error happens. This is called
     * after the render function.
     *
     * @param error - The error that occurred.
     * @param errorInfo - The info about the error that occurred.
     */
    componentDidCatch(error: Error, errorInfo: ErrorInfo) {

        console.error("Uncaught error:", error, errorInfo);
        // You can also log the error to an error reporting service
        // e.g. logErrorToMyService(error, errorInfo);
    }

    render() {

        if (this.state.hasError) {

            return (

                <Alert className={"my-4"}
                       variant={"danger"}
                >
                    Something went wrong
                </Alert>
            )
        }

        return this.props.children;
    }
}