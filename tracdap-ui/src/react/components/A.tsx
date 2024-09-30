/**
 * A component that makes text look like an HTML hypertext link. The children of the component
 * is the text that will be shown as the link. This component should be used for external links,
 * TRAC-UI uses react-router to navigate internal links.
 *
 * @module A
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the A component.
 */
export interface Props {

    /**
     * The css class to apply to the link, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The link to point to.
     */
    href: string
}

export const A = (props: React.PropsWithChildren<Props>) => {

    const {children, className, href} = props;

    return (

        <a className={`link-info ${className || ""}`} rel={"noreferrer noopener"} target="_blank" href={href}>{children}</a>
    )
};

A.propTypes = {

    children: PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.element]).isRequired,
    className: PropTypes.string,
    href: PropTypes.string.isRequired
};