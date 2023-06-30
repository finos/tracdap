/**
 * A component that shows an icon in the user interface. This uses the bootstrap icon
 * package, icons are shown as font icons.
 *
 * @module Icon
 * @category Component
 */

import type {CSSProperties} from "react";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * An interface for the props of the Icon component.
 */
export interface Props {

    /**
     * The label for accessibility screen readers. If false then the aria is hidden, however if this is the
     * case you should add the aria-label to the icon's parent component e.g. when using an icon in a button.
     */
    ariaLabel: string | false
    /**
     * The css class to apply to the icon, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The hexadecimal colour for the icon. If you want to make the icon have a colour via a class then set this
     * to null. Setting color to null means that the color will be set by the class rather than inherited
     * by default.
     */
    colour?: null | string
    /**
     * The icon to show. This is a string referencing a name from the {@link https://icons.getbootstrap.com/|Bootstrap icons} project.
     */
    icon: string
    /**
     * The placement of the tooltip relative to the icon. In some cases the DOM will not allow a tool tip to show at the selected
     * location so if you use tooltips check that they can be rendered.
     */
    placement?: 'top' | 'bottom' | 'left' | 'right'
    /**
     * The size of the icon as a string with units e.g. "1rem". If this is not set the default is to inherit the size from the parent
     * font size.
     */
    size?: string
    /**
     * The tooltip text to show when the icon is hovered over.
     */
    tooltip?: string
}

const IconInner = (props: Props) => {

    const {
        ariaLabel,
        className = "",
        colour = "inherit",
        icon,
        placement = "top",
        size = "inherit",
        tooltip
    } = props;

    // All bootstrap icons need the bi class
    const finalClassName = `bi ${className} ${icon}`

    // The css style object to pass to the SVG
    const style: CSSProperties = {
        fontSize: size,
        color: !colour ? undefined : colour
    }

    return (

        tooltip ?

            // The undefined props is because of an eslint bug I think that requires they be set
            <OverlayTrigger placement={placement}
                            overlay={<Tooltip id={"icon-tooltip"}>{tooltip}</Tooltip>}
                            // There is a bug in eslint or this component's interface that says these are required
                            defaultShow={undefined}
                            delay={undefined}
                            flip={undefined}
                            onHide={undefined}
                            onToggle={undefined}
                            popperConfig={undefined}
                            show={undefined}
                            target={undefined}
                            trigger={undefined}
            >
                <i aria-hidden={typeof ariaLabel === "string" ? "false" : "true"}
                   aria-label={typeof ariaLabel === "string" ? ariaLabel : undefined}
                   className={finalClassName}
                   role={"img"}
                   style={style}
                />
            </OverlayTrigger>
            :
            <i aria-hidden={typeof ariaLabel === "string" ? "false" : "true"}
               aria-label={typeof ariaLabel === "string" ? ariaLabel : undefined}
               className={finalClassName}
               role={"img"}
               style={style}
            />
    )
};

IconInner.propTypes = {

    ariaLabel: PropTypes.oneOfType([PropTypes.string, PropTypes.bool]),
    className: PropTypes.string,
    colour: PropTypes.string,
    icon: PropTypes.string.isRequired,
    placement: PropTypes.oneOf(['top', 'bottom', 'left', 'right']),
    size: PropTypes.string,
    tooltip: PropTypes.string
};

// We only rerender if the icon or the colour changes
export const Icon = memo(IconInner, (prevProps, nextProps) => prevProps["icon"] === nextProps["icon"] && prevProps["colour"] === nextProps["colour"]);