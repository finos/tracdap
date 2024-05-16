/**
 * A component that simply handles the rendering of its child components. This means that we can set the
 * state on these children if they are open or closed. This is the magic of this recursive component.
 *
 * @module FileTree
 * @category Component
 */

import {Icon} from "../Icon";
import React, {useReducer} from "react";

/**
 * A style for the indent of a file inside it'd parent folder, storing it like this means that we don't trigger
 * re-renders when using it as a prop.
 */
const indent = {"marginLeft": "1.75rem"}

/**
 * The reducer function that runs when the folder icon in the file tree is clicked on and there is an update to
 * state. It contains the ability to perform a range of types of updates.
 * @param prevState - The component state before the update.
 * @param action - The action and the values to update in state.
 */
type Action = { type: 'addIsOpen', label: string } | { type: 'resetIsOpen', label: string }

/**
 * The reducer function that runs when component updates its state.
 *
 * @param prevState - The component state before the update.
 * @param action - The action and the values to update in state.
 */
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
            throw new Error("Reducer method not recognised")
    }
}

/**
 * An interface for the props of the SubTree component.
 */
export interface Props {

    /**
     * Whether multiple menu items can be expanded at the same time. If not then opening one folder will close
     * any already open folders.
     */
    allowMultipleMenusOpen: boolean
    /**
     * The folders within the branch.
     */
    children: React.ReactElement | React.ReactElement[]
    /**
     * Whether to show folders and files labelled as hidden/unselectable. These are empty folders for
     * example or files that are not eligible for some process further down such as loading as a
     * dataset.
     */
    showUnselectableFiles: boolean
}

/**
 * An interface for the state of the SubTree component.
 */
type State = Record<string, boolean>

export const SubTree = (props: Props) => {

    // These props are the props of the SubTree component not the children
    const {children, allowMultipleMenusOpen, showUnselectableFiles} = props

    /**
     * A function that runs when the component mounts and that allows us to set the initial state using a function.
     * @param initState - The initial state for the component as defined in the useReducer function. This
     * function adds to that state.
     */
    const init = (initState: State): State => {

        let openSections: State = {}

        children && React.Children.forEach(children, child => {

            if (child["props"]["openOnLoad"]) {
                openSections[child["props"].title] = true
            }
        })

        return {...initState, ...openSections}
    }

    const [openSections, updateOpenSections] = useReducer(reducer, {}, init)

    /**
     * A function that runs when the user clicks on a folder item that opens to show a submenu.
     * This updates the state so that we track wht items are open or closed.
     * @param event - The event that triggered the function.
     */
    const onClick = (event: React.MouseEvent<HTMLDivElement>) => {

        const folderName = event.currentTarget.getAttribute("data-folder-name")

        if (folderName !== null) {
            updateOpenSections({type: allowMultipleMenusOpen ? "addIsOpen" : "resetIsOpen", label: folderName})
        }
    }

    return (

        <React.Fragment>
            {children && React.Children.map(children, (child, i) => {

                // Get the props of the children (not the SubTree component)
                const {props: {title, isTreeRoot, isExpandable, selectable, setFolderClickCount}} = child
                const isOpen = openSections[title]

                // If the child component has an isExpandable prop then we show a folder icon otherwise we show a div
                // with the file icon in it.
                if (isExpandable) {

                    return (
                        <div key={i} style={!isTreeRoot ? indent : undefined}
                             className={!selectable && !showUnselectableFiles ? "folder d-none" : "folder my-2"}>
                            <div id={title} data-folder-name={title} onClick={e => {
                                onClick(e);
                                setFolderClickCount((prevState: number) => prevState + 1)
                            }}
                                 className={"fs-9 d-flex align-items-center"}>
                                <Icon ariaLabel={"Folder"}
                                      icon={`pointer ${isOpen ? (selectable ? "bi-folder2-open" : "bi-folder2-open") : (selectable ? "bi-folder" : "bi-folder-fill")}`}
                                      className={"me-3"}
                                />
                                <span className={"fs-8 pointer"}>{title}</span>
                            </div>

                            {/*Hide or show the children to the expandable menu*/}
                            <div className={`${isOpen ? "expandable-show" : "expandable-hide"}`}>
                                {child}
                            </div>
                        </div>
                    )

                } else {

                    return (

                        <div style={!isTreeRoot ? indent : undefined} key={i}>
                            {child}
                        </div>
                    )
                }
            })}
        </React.Fragment>
    )
}