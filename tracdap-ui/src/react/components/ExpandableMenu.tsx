/**
 * A component that shows an expandable item in the side menu.
 *
 * @module ExpandableMenu
 * @category Component
 */

import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React, {memo, useReducer} from "react";
import type {ThemesList} from "../../types/types_general";

/**
 * The reducer function that runs when this component updates its state.
 * @param prevState - The component state before the update.
 * @param action - The action and the values to update in state.
 */
type Action = { type: 'addIsOpen' | 'resetIsOpen', label: string }

function reducer(prevState: State, action: Action) {

    switch (action.type) {

        case "addIsOpen":

            return ({
                ...prevState,
                [action.label]: !prevState[action.label],
            })

        case "resetIsOpen":

            return ({[action.label]: !prevState[action.label]})

        default:
            throw new Error(`Reducer method is not recognised`)
    }
}

/**
 * An interface for the props of the ExpandableMenu component.
 */
export interface Props {

    /**
     * Whether multiple menu items can be expanded at the same time.
     */
    allowMultipleMenusOpen: boolean
    /**
     * The links to show within the expandable menu when the menu is opened.
     */
    children: React.ReactElement<any, any>[] | React.ReactElement<any, any>
    /**
     * The theme selected by the user, this is passed as a prop rather than
     * retrieved from the store, so we can reduce rendering by using it in
     * the memo isEqual function.
     */
    theme: ThemesList
}

/**
 * An interface for the state of the ExpandableMenu component.
 */
export interface State {
    [key: string]: boolean
}

const ExpandableMenuInner = (props: Props) => {

    const {allowMultipleMenusOpen, children, theme} = props

    /**
     * A function that runs when the component mounts and that allows us to set the initial state using a function. In
     * this case setting which menu sections are open.
     * @param initState - The initial state for the component.
     */
    const init = (initState: State) => {

        let openSections: State = {}

        React.Children.forEach(children, child => {

            if (child.props["openOnLoad"]) {
                openSections[child.props.title] = true
            }
        })

        return {...initState, ...openSections}
    }

    // The state initializer and the reducer function to update it
    const [openSections, updateOpenSections] = useReducer(reducer, {}, init)

    /**
     * A function that runs when the user clicks on a menu item that opens to show a submenu.
     * This updates the state so that we track wht items are open or closed.
     * @param event - The event that triggered the function.
     */
    const onClick = (event: React.MouseEvent<HTMLDivElement>): void => {

        if (allowMultipleMenusOpen) {

            updateOpenSections({type: "addIsOpen", label: event.currentTarget.id})

        } else {

            updateOpenSections({type: "resetIsOpen", label: event.currentTarget.id})
        }
    }

    return (
        <React.Fragment>
            {React.Children.map(children, (child, i) => {

                const isOpen = openSections[child.props.title]

                if (child.props.isExpandable) {

                    return (
                        <React.Fragment key={i}>
                            <div className={`side-menu-expandable-header ${theme} d-flex py-1`}
                                 id={child.props.title}
                                 onClick={onClick}
                            >
                                <Icon ariaLabel={false}
                                      className={"mx-3"}
                                      icon={isOpen ? "bi-chevron-down" : "bi-chevron-right"}
                                      size={"1.25rem"}
                                />
                                <span className={"my-auto"}>{child.props.title}</span>
                            </div>

                            {/*Hide or show the children to the expandable menu*/}
                            <div className={`ps-4 ms-3 ${isOpen ? "expandable-show" : "expandable-hide"}`}>
                                {child}
                            </div>
                        </React.Fragment>
                    )

                } else {
                    return <div key={i}>{child}</div>
                }
            })}
        </React.Fragment>
    )
};

ExpandableMenuInner.propTypes = {

    allowMultipleMenusOpen: PropTypes.bool.isRequired,
    children: PropTypes.oneOfType([PropTypes.node, PropTypes.arrayOf(PropTypes.node)]).isRequired,
    theme: PropTypes.string.isRequired
};

export const ExpandableMenu = memo(ExpandableMenuInner, (prevProps, nextProps) => prevProps.theme === nextProps.theme)