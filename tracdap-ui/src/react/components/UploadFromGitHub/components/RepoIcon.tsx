/**
 * A component that shows an icon and a counter for a property of the selected repository e.g. its contributor count.
 * Its added as a separate component the RepositoryInfo component as there are multiple icons.
 *
 * @module RepoIcon
 * @category Component
 */

import {A} from "../../A";
import {Icon} from "../../Icon";
import PropTypes from "prop-types";
import React from "react";

/**
 * An interface for the props of the RepoIcon component.
 */
export interface Props {

    /**
     * The css class to apply to the widget, this allows additional styles to be added to the icon.
     */
    className?: string
    /**
     * The number of events (e.g. branches) to display in the widget
     */
    count: number
    /**
     * The link that the widget should point to.
     */
    href: string
    /**
     * The icon to show.
     */
    icon: string
    /**
     * The label to show with the widget.
     */
    label: string
}

export const RepoIcon = (props: Props) => {

    const {className = "", count, href, icon, label} = props

    return (
        <div className={`${className} d-flex align-items-center ps-3 text-tertiary`}>
            <div>
                <Icon ariaLabel={label}
                      icon={icon}
                />
            </div>
            <div className={"ps-2 d-flex flex-column lh-1"}>
                <A className={"fs-11 git git-link"}
                   href={href}
                >
                    {count}
                </A>
                <div className={"fs-8"}>{label}</div>
            </div>
        </div>
    )
};

RepoIcon.propTypes = {

    className: PropTypes.string,
    count: PropTypes.number.isRequired,
    href: PropTypes.string.isRequired,
    icon: PropTypes.string.isRequired,
    label: PropTypes.string.isRequired
};