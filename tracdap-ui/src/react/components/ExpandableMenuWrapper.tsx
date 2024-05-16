/**
 * A component that wraps the ExpandableMenu component. This is used as
 * a rendering optimisation. By memorizing this component and only updating
 * if the theme changes we prevent the whole menu tree re-rendering when the
 * side menu is opened or closed.
 *
 * @module ExpandableMenuWrapper
 * @category Component
 */

import ExpandableHeader from "./ExpandableHeader";
import {ExpandableMenu} from "./ExpandableMenu";
import {Icon} from "./Icon";
import {Menu} from "../../config/config_menu"
import {NavLink} from "react-router-dom";
import PropTypes from "prop-types";
import React, {memo} from "react";
import type {ThemesList} from "../../types/types_general";
import {toggleSideMenu} from "../store/applicationStore";
import {useAppDispatch} from "../../types/types_hooks";

/**
 * An interface for the props of the ExpandableHeaderWrapper component.
 */
export interface Props {

    /**
     * The theme selected by the user, this is passed as a prop rather than
     * retrieved from the store, so we can reduce rendering by using it in
     * the memo isEqual function.
     */
    theme: ThemesList
}

const ExpandableMenuWrapperInner = (props: Props) => {

    const {theme} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    return (
        <ExpandableMenu allowMultipleMenusOpen={true} theme={theme}>

            {Menu.filter(menuItem => !menuItem.hiddenInSideMenu).map(menuItem => {

                if (!menuItem.expandableMenu) {

                    // The menu allows that multiple paths point to the same page, but only one of these can be shown in the menu
                    // this is for the viewer components where they need to have URL parameters
                    const path = Array.isArray(menuItem.path) ? menuItem.path?.[0] : menuItem.path

                    return (
                        <NavLink className={`d-flex side-menu-nav-link ${theme} py-2 my-1`}
                                 key={path}
                                 onClick={() => dispatch(toggleSideMenu())}
                                 to={path}
                            // See https://stackoverflow.com/questions/70644361/react-router-dom-v6-shows-active-for-index-as-well-as-other-subroutes
                                 end={["/", ""].includes(path) ? true : undefined}
                        >
                            <Icon ariaLabel={false}
                                  className={"mx-3 my-auto"}
                                  icon={menuItem.icon || "bi-hexagon"}
                                  size={"120%"}
                            />
                            <span className={"my-auto"}>{menuItem.title}</span>
                        </NavLink>
                    )

                } else {

                    // The menu allows that multiple paths point to the same page, but only one of these can be shown in the menu
                    // this is for the viewer components where they need to have URL parameters
                    const path = Array.isArray(menuItem.path) ? menuItem.path?.[0] : menuItem.path

                    return (
                        <ExpandableHeader isExpandable={true}
                                          key={path}
                                          openOnLoad={menuItem.openOnLoad}
                                          title={menuItem.title}
                        >
                            {menuItem.children.filter(subMenuItem => !subMenuItem.hiddenInSideMenu).map(subMenuItem => (
                                <NavLink className={`d-flex side-menu-nav-link ${theme} py-2 my-1`}
                                         key={`${menuItem.path}/${subMenuItem.path}`}
                                         onClick={() => dispatch(toggleSideMenu())}
                                         to={`${menuItem.path}/${subMenuItem.path}`}
                                >
                                    <Icon ariaLabel={false}
                                          className={"mx-2 my-auto"}
                                          icon={subMenuItem.icon || "bi-hexagon"}
                                    />

                                    <span className={"my-auto"}>{subMenuItem.title}</span>
                                </NavLink>
                            ))}

                        </ExpandableHeader>
                    )
                }
            })}
        </ExpandableMenu>
    )
}

ExpandableMenuWrapperInner.propTypes = {

    theme: PropTypes.string.isRequired
}

export const ExpandableMenuWrapper = memo(ExpandableMenuWrapperInner, (prevProps, nextProps) => prevProps.theme === nextProps.theme);
