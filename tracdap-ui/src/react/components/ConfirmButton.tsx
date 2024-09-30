/**
 * A component that shows a button that when clicked a confirm dialog box is shown that the user has to approve for the
 * button to do anything. This is really just combining two other components together in a single component but the
 * addition of the handleClick function which can handle dispatching actions means that there is no need for wrapper
 * functions to be sprinkled everywhere a button needs to dispatch an action.
 *
 * @module ConfirmButton
 * @category Component
 */

import type {ActionCreatorWithPayload, AsyncThunk} from "@reduxjs/toolkit";
import {Button} from "./Button";
import type {ButtonPayload, ConfirmButtonPayload, Variants} from "../../types/types_general";
import Confirm from "./Confirm";
import PropTypes from "prop-types";
import React from "react";
import type {RootState} from "../../storeController";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the ConfirmButton component.
 */
export interface Props {

    /**
     * The label for accessibility screen readers.
     */
    ariaLabel: string
    /**
     * The text on the button to cancel the action.
     */
    cancelText?: string
    /**
     * The css class to apply to the icon, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The text on the button to confirm the action.
     */
    confirmText?: string
    /**
     * The main text in the confirmation modal, asking the user if they want to continue.
     */
    description?: string
    /**
     * Whether the button is disabled.
     */
    disabled?: boolean
    /**
     * The function to run when the button is clicked on, this is for this function will be dispatched to a store.
     */
    dispatchedOnClick?: ActionCreatorWithPayload<ConfirmButtonPayload> | AsyncThunk<any, ConfirmButtonPayload, { state: RootState }>
    /**
     * Error messages to show in the confirmation modal, this disables the modal.
     */
    errorMessage?: string | string[]
    /**
     * The id of the button, used for passing back information about what was clicked through the handleClick function.
     */
    id?: string | number
    /**
     * Whether to ignore the need to show the pop-up. For example if it should only be shown conditional on some other
     * action then this can be used set the condition.
     */
    ignore?: boolean
    /**
     * The index of the button in a list, used for passing back information about what was clicked through the handleClick function.
     */
    index?: number
    /**
     * The name of the button, used for passing back information about what was clicked through the handleClick function.
     */
    name?: string | number
    /**
     * The function to run when the button is clicked on. This is for when the function does not need to be dispatched.
     */
    onClick?: (payload : ConfirmButtonPayload) => void
    /**
     * The size of the button.
     */
    size?: "sm" | "lg"
    /**
     * The title at the top of the confirmation modal.
     */
    title?: string
    /**
     * The variant or bootstrap styling to apply to the button.
     */
    variant?: Variants
    /**
     * Warning messages to show in the confirmation modal, this does not disable it.
     */
    warningMessage?: string | string[]
}

export const ConfirmButton = (props: React.PropsWithChildren<Props>) => {

    const {
        ariaLabel,
        cancelText,
        children,
        className,
        confirmText,
        description,
        disabled = false,
        dispatchedOnClick,
        errorMessage,
        id,
        ignore,
        index,
        name,
        onClick,
        size,
        title,
        variant = "info",
        warningMessage
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user clicks on the button and then confirms the action.
     */
    const handleClick = (): void => {

        const payload = {id, name, index}

        // This passes id and the name props back to the onClick function and deals with
        // whether the function need to dispatch and action to the redux store.
       dispatchedOnClick ? dispatch(dispatchedOnClick(payload)) : onClick ? onClick(payload) : undefined
    }

    return (

        <Confirm cancelText={cancelText}
                 confirmText={confirmText}
                 description={description}
                 errorMessage={errorMessage}
                 ignore={ignore}
                 isDispatched={false}
                 title={title}
                 warningMessage={warningMessage}
        >
            {(confirm: (arg: any) => ActionCreatorWithPayload<ButtonPayload> | ((payload: ButtonPayload) => any)) => (

                <Button ariaLabel={ariaLabel}
                        className={className}
                        onClick={confirm(handleClick)}
                        size={size}
                        variant={variant}
                        isDispatched={false}
                        passOnClick={true}
                        disabled={disabled}
                >
                    {children}
                </Button>
            )}
        </Confirm>
    )
};

ConfirmButton.propTypes = {

    Label: PropTypes.string,
    cancelText : PropTypes.string,
    children: PropTypes.oneOfType([
        PropTypes.string,
        PropTypes.element,
        PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.element.isRequired]))
    ]),
    className: PropTypes.string,
    confirmText: PropTypes.string,
    description: PropTypes.string,
    disabled: PropTypes.bool,
    dispatchedOnClick: PropTypes.func,
    errorMessage: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    ignore: PropTypes.bool,
    index: PropTypes.number,
    name: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    onClick: PropTypes.func,
    size: PropTypes.oneOf(["sm", "lg"]),
    title: PropTypes.string,
    ariaLabel: PropTypes.string.isRequired,
    variant: PropTypes.string,
    warningMessage: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)])
};