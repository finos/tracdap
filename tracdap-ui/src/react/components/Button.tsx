/**
 * A component that shows a button, this is just a wrapper to the Bootstrap Button component with the preset classes
 * applied and additional features. The addition of the handleClick function which can handle dispatching actions means
 * that there is no need for wrapper functions to be sprinkled everywhere a button needs to dispatch an action.
 *
 * @module Button
 * @category Component
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import BootstrapButton from "react-bootstrap/Button";
import type {ButtonPayload, SizeList, Variants} from "../../types/types_general";
import PropTypes from "prop-types";
import React from "react";
import Spinner from "react-bootstrap/Spinner";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the Button component.
 */
export interface Props {

    /**
     * Whether the button should show an active state.
     */
    active?: boolean
    /**
     * The label for accessibility screen readers.
     */
    ariaLabel: string
    /**
     * The css class to apply to the icon, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether the button is disabled.
     */
    disabled?: boolean
    /**
     * The function to run when the button is clicked on, this function will be dispatched.
     */
    dispatchedOnClick?: ActionCreatorWithPayload<ButtonPayload>
    /**
     * A link to navigate to with the button click.
     */
    href?: string
    /**
     * The id of the button, used for passing back information about what was clicked through the handleClick function.
     */
    id?: string | number
    /**
     * The index of the button in a list, used for passing back information about what was clicked through the handleClick function.
     */
    index?: number
    /**
     * Whether the action for the form element needs to be dispatched or not. Thunks
     * need to be dispatched.
     */
    isDispatched?: boolean
    /**
     * Whether the button should show a loading icon, for example during an API call.
     */
    loading?: boolean
    /**
     * The name of the button, used for passing back information about what was clicked through the handleClick function.
     */
    name?: string
    /**
     * The function to run when the button is clicked on. It is not required as href can be used instead.
     */
    onClick?: Function
    /**
     * Whether to pass the onClick function directly to the button and skip the handleClick function wrapper. This is
     * needed if this component is being used by the ConfirmButton component. In this case the handling of the click
     * is performed in the ConfirmButton component so the button needs to ignore its own version of this.
     */
    passOnClick?: boolean
    /**
     * The size of the button.
     */
    size?: SizeList
    /**
     * The variant or bootstrap styling to apply to the button.
     */
    variant?: Variants
}

/**
 * A component that shows a button, this is just a wrapper to the Bootstrap Button component with the preset classes
 * applied and additional features. The addition of the handleClick function which can handle dispatching actions means
 * that there is no need for wrapper functions to be sprinkled everywhere a button needs to dispatch an action.
 */

export const Button = (props: React.PropsWithChildren<Props>) => {

    const {
        active = false,
        ariaLabel,
        children,
        className = "min-width-px-100",
        disabled = false,
        dispatchedOnClick,
        id,
        index,
        isDispatched = true,
        loading = false,
        name,
        onClick,
        passOnClick,
        size,
        variant = "info",
        href
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user clicks on the button.
     */
    const handleClick = (e: React.MouseEvent<HTMLButtonElement>): void => {

        // Prevent any on click events in the div behind the one clicked on
        if (e && e.stopPropagation) e.stopPropagation();

        const payload = {id, index, name}

        // This passes id, name amd packet props back to the onClick function and deals with
        // whether the function need to dispatch and action to the redux store.
        isDispatched && !dispatchedOnClick && onClick ? dispatch(onClick(payload)) : isDispatched && dispatchedOnClick ? dispatch(dispatchedOnClick(payload)) : onClick ? onClick(payload) : undefined
    }

    return (

        <BootstrapButton active={active}
                         aria-label={ariaLabel}
                         className={className}
                         disabled={disabled || loading}
                         href={href}
                         onClick={passOnClick ? () => onClick !== undefined  && onClick({id, name}) : handleClick}
                         size={size}
                         variant={variant}
            // If target is set the button can not be disabled
                         target={href ? "_blank" : undefined}
        >
            {loading ?
                <Spinner animation="border"
                         aria-hidden="true"
                         role="status"
                         size="sm"
                >
                    <span className="visually-hidden">Loading...</span>
                </Spinner>
                : children}

        </BootstrapButton>
    )
};

Button.propTypes = {

    active: PropTypes.bool,
    ariaLabel: PropTypes.string.isRequired,
    // The number type in the array covers when the child is null
    children: PropTypes.oneOfType([
        PropTypes.string,
        PropTypes.element,
        PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.element.isRequired]))
    ]),
    className: PropTypes.string,
    disabled: PropTypes.bool,
    dispatchedOnClick: PropTypes.func,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    index: PropTypes.number,
    isDispatched: PropTypes.bool,
    loading: PropTypes.bool,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    onClick: PropTypes.func,
    passOnClick: PropTypes.bool,
    size: PropTypes.oneOf(["lg", "sm"]),
    variant: PropTypes.string
};