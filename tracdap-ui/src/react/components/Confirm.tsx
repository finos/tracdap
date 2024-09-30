/**
 * A component that shows a confirmation pop up when the user clicks on a button or any
 * action that you want to add a step to before continuing. For more information see
 * {@link https://itnext.io/add-confirmation-dialog-to-react-events-f50a40d9a30d| this guide}.
 *
 * Note that if you want to dispatch an action that is being controlled by the Confirm
 * component then set the isDispatched prop on the Confirm component to true and set isDispatched to false
 * on the child component.
 *
 * @module Confirm
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {Button} from "./Button";
import type {ButtonPayload, Option, SelectPayload} from "../../types/types_general";
import {MessageList} from "./MessageList";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React, {memo, useState} from "react";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the Confirm component.
 */
export interface Props {

    /**
     * The text on the button to cancel the action.
     */
    cancelText?: string
    /**
     * Children added between the <Confirm></Confirm> tags.
     */
    children: (callback: (arg: any) => ActionCreatorWithPayload<ButtonPayload> | ((payload: ButtonPayload) => any)) => React.ReactElement<any, any>
    /**
     * The text on the button to confirm the action.
     */
    confirmText?: string
    /**
     * The main text in the confirmation modal, asking the user if they want to continue.
     */
    description?: string
    /**
     * Error messages to show in the confirmation modal, this disables the modal.
     */
    errorMessage?: string | string[]
    /**
     * Whether to ignore the need to show the pop-up. For example if it should only be shown conditional on some other
     * action then this can be used set the condition.
     */
    ignore?: boolean
    /**
     * Whether the action needs to be dispatched or not. Thunks need to be dispatched.
     */
    isDispatched?: boolean
    /**
     * The title at the top of the confirmation modal.
     */
    title?: string
    /**
     * Warning messages to show in the confirmation modal, this does not disable it.
     */
    warningMessage?: string | string[]
}

export const Confirm = (props: Props) => {

    const {
        cancelText = 'Cancel',
        children,
        confirmText = 'Confirm',
        description = 'Are you sure?',
        errorMessage,
        isDispatched = false,
        ignore = false,
        title = 'Please confirm',
        warningMessage
    } = props;

    // Set the state variables
    const [showConfirm, setShowConfirm] = useState(false)
    const [callback, setCallback] = useState(() => () => {
    })

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that is passed as an argument to the function that is a child this component and is run when
     * the user clicks on the component that the children renders.
     *
     * <Confirm>{confirm => (<Button onClick={confirm(() => {alert('dude')})}>Click me</Button>)}</Confirm>
     *
     * In the example above the show function is equivalent to the confirm function that takes confirm as the argument
     * and returns the Button component. It's passed into the child (everything between the component tags) and acts
     * as a wrapper to the actual function you want to run if the user clicks confirms the action. This means that we
     * render the children but, we intercept the action attached to the button, and we attach that action to a button
     * in the confirmation component.
     * @param callback - The original function that the user triggered.
     */
    const show: (callback: Function) => (event: ButtonPayload | SelectPayload<Option, boolean>) => void = (callback: Function) => (payload: ButtonPayload | SelectPayload<Option, boolean>) => {

        // Show the confirmation modal
        if (!ignore) setShowConfirm(true)

        // Store the function that was going to fire with the event
        // normally this callback is an onClick function
        if (isDispatched) {

            // We store a function in the store that returns a function that dispatches the original event
            // We need to do this because we set the arguments and without the first function the
            // second function would be called.
            const dispatchAction: () => any = () => () => dispatch(callback(payload))

            // We can't send back the original event as its non-serializable and redux prints
            // a warning, so instead we send some common attributes
            if (ignore) {
                dispatchAction()()
            } else {
                setCallback(dispatchAction);
            }

        } else {

            // See note above
            const nonDispatchAction: () => any = () => () => callback(payload)

            if (ignore) {
                nonDispatchAction()()
            } else {
                setCallback(nonDispatchAction);
            }
        }
    }

    /**
     * A function that hides the confirmation modal and resets the state.
     */
    const hide: () => void = () => {

        setShowConfirm(false)
        setCallback(() => () => {
        })
    }

    /**
     * A function that runs when the user clicks 'OK' in the confirmation modal, it runs the
     * original function associated with the event and hides the confirmation modal.
     */
    const confirm: () => void = () => {

        // Callback is the function stored in state
        callback()
        hide()
    }

    const hasErrorMessages: boolean = Boolean(errorMessage != null && errorMessage.length > 0);
    const hasWarningMessages: boolean = Boolean(warningMessage != null && warningMessage.length > 0);

    return (

        <React.Fragment>

            {/* This renders the children returned by the function defined between the <Confirm></Confirm> tags. */}
            {/* The show function is passed as an argument and used to run when the button is clicked*/}
            {children(show)}

            <Modal centered={true}
                   onHide={hide}
                   show={showConfirm}
                // Make the modal bigger if there are messages to show
                   size={(hasErrorMessages || hasWarningMessages) ? "lg" : "sm"}
            >

                <Modal.Header closeButton>
                    <Modal.Title>
                        {title}
                    </Modal.Title>
                </Modal.Header>

                <Modal.Body>
                    {description}

                    {hasErrorMessages && errorMessage &&
                        <MessageList centerText={Boolean(!Array.isArray(errorMessage))}
                                     className={'mt-4'}
                                     listHasHeader={false}
                                     messages={errorMessage}
                                     variant={"danger"}
                        />
                    }

                    {hasWarningMessages && warningMessage &&
                        <MessageList centerText={Boolean(!Array.isArray(warningMessage))}
                                     className={'mt-4'}
                                     listHasHeader={false}
                                     messages={warningMessage}
                                     variant={"warning"}
                        />
                    }
                </Modal.Body>

                <Modal.Footer>

                    <Button ariaLabel={"Cancel"}
                            isDispatched={false}
                            onClick={hide}
                            variant={"secondary"}>
                        {cancelText}
                    </Button>

                    <Button ariaLabel={"Confirm"}
                            isDispatched={false}
                            disabled={hasErrorMessages}
                            variant={"info"}
                            onClick={confirm}
                    >
                        {confirmText}
                    </Button>

                </Modal.Footer>
            </Modal>
        </React.Fragment>
    )
};

Confirm.propTypes = {

    cancelText: PropTypes.string,
    children: PropTypes.any.isRequired,
    confirmText: PropTypes.string,
    errorMessage: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
    description: PropTypes.string,
    ignore: PropTypes.bool,
    isDispatched: PropTypes.bool,
    title: PropTypes.string,
    warningMessage: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
};

export default memo(Confirm);