/**
 * A component that shows a three line burger icon that changes to a cross icon
 * when clicked. This is used to add a menu into the user interface.
 *
 * @module Burger
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";

/**
 * Interface for the props of the Burger component.
 */
export interface Props {

    /**
     * The label for accessibility screen readers.
     */
    ariaLabel: string
    /**
     * A function to run when the burger is clicked. The onClick is optional as this is used in a button in some
     * components and the button handles the onClick event.
     */
    onClick?: React.MouseEventHandler<HTMLDivElement>
    /**
     * Whether the button shows the burger or the cross icon.
     */
    open: boolean
    /**
     * The size of the burger, there are css classes for each of these.
     */
    size: "sm" | "md" | "lg"
}

export const Burger = ({ariaLabel, onClick, open, size}: Props) => {

    return (

        <div aria-label={ariaLabel}
             className={`my-auto burger burger-${size} ${open ? 'open' : 'closed'}`}
             onClick={onClick}
        >
            <div/>
            <div/>
            <div/>
        </div>
    )
};

Burger.propTypes = {

    ariaLabel: PropTypes.string.isRequired,
    onClick: PropTypes.func,
    open: PropTypes.bool.isRequired,
    size: PropTypes.oneOf(["sm", "md", "lg"]).isRequired
};