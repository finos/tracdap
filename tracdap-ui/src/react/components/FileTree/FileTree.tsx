/**
 * A component that shows an expandable file tree, it is a recursive component, calling itself for each branch in the
 * file tree. This is used in {@link UploadAModelScene} as well as other scenes loading from repos.
 *
 * @module FileTree
 * @category Component
 */

import ExpandableFolder from "./ExpandableFolder";
import {File} from "./File";
import type {FileTree as FileTreeType, FolderTree, PopupDetails, SelectedFileDetails} from "../../../types/types_general";
import {hasOwnProperty} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {memo, useState} from "react";
import {SubTree} from "./SubTree";

/**
 * An interface for the props of the FileTree component.
 */
export interface Props {

    /**
     * Whether multiple menu items can be expanded at the same time. If not then opening one folder will close
     * any already open folders.
     */
    allowMultipleMenusOpen: boolean
    /**
     * The file tree object. This is a recursive object mapping the files and the folders they are in.
     */
    fileTree: FileTreeType
    /**
     * A count for the number of clicks on folders made by the user, we do this as part of optimising the rendering of
     * this component. When someone clicks on a file a popup is shown next to it to indicate that it has been selected.
     * However, when the user then licks on a folder the position of the file icon and label moves but popup stays where
     * it is. This is because we have memoized the components to only re-render on certain events. So we add a state
     * variable that is the count of user clicks on folders, and we will add a rule that says if a file is selected
     * and the folder click count changes then the popup should be redrawn, so it moves with the file icon and label.
     */
    folderClickCount?: number
    /**
     * The folder tree object. This is a recursive object mapping the folders.
     */
    folderTree: FolderTree
    /**
     * Whether the branch being rendered is the first node in the tree or a latter one, this allows us to indent
     * branches for each tier but have the first tier unindented. This prop is undefined at the root of the tree.
     * @defaultValue true
     */
    isTreeRoot?: boolean
    /**
     * The folder path e.g. "impairment/models" that the file tree branch is for. This is built up by passing this
     * prop down each branch and adding to it as the FileTree component is recursively rendered. The folderTree
     * prop is keyed by this path so at every node we use this location to look up the information about the folder.
     * For example does the folder have files in it or any of it sub folders that can be selected. This prop is
     * false at the root of the tree.
     */
    location?: string
    /**.
     * A function that runs when the user clicks on a file in the tree, this passes back
     * information about the selected file to be passed to a parent component
     */
    onSelectFile: React.Dispatch<React.SetStateAction<null | SelectedFileDetails>>
    /**
     * The state of the popup messages.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to only enable a single pop up message in the user interface.
     */
    popup?: PopupDetails,
    /**
     * The details of the selected file. This is the path property of the file and is a unique reference
     * to the selected file (the file name is may not be unique across the tree). This is used to
     * select which PopOver overlay to show.
     */
    selectedFileDetails: null | SelectedFileDetails,
    /**
     * A function to update the values associated with the number of folder clicks done by the user.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to redraw pop up messages in the user interface when their position should
     * change.
     */
    setFolderClickCount?: React.Dispatch<React.SetStateAction<number>>
    /**
     * A function to update the values associated with the popups.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to only enable a single pop up message in the user interface.
     */
    setPopup?: React.Dispatch<React.SetStateAction<PopupDetails>>
    /**
     * Whether to show folders and files labelled as hidden/unselectable. These are empty folders for
     * example or files that are not eligible for some process further down such as loading as a
     * dataset.
     */
    showUnselectableFiles: boolean,
    /**
     * An array of folder paths for folders that should be open when the fileTree mounts.
     * @defaultValue []
     */
    openFoldersOnLoad?: string[]
}

// The default function for the openFoldersOnLoad prop. This is defined outside the component
// in order to prevent re-renders.
const defaultOpenFoldersOnLoad : Props["openFoldersOnLoad"]= []

const FileTreeInner = (props: Props) => {

    const {
        allowMultipleMenusOpen,
        fileTree,
        folderClickCount: folderClickCountProp,
        folderTree,
        location,
        isTreeRoot = true,
        openFoldersOnLoad = defaultOpenFoldersOnLoad,
        onSelectFile,
        popup: popupProp,
        selectedFileDetails,
        setFolderClickCount: setFolderClickCountProp,
        setPopup: setPopupProp,
        showUnselectableFiles
    } = props

    /**
     *   Set some state information about the popups shown to the user for selectable and unselectable files. This is only
     *   set at the root of the tree and after that the state variables and updater functions are passed as props to
     *   branches
     */
    const [popupState, setPopupState] = useState<PopupDetails>({
        selectable: {
            path: selectedFileDetails?.path ? selectedFileDetails.path : "",
            show: Boolean(selectedFileDetails?.path && selectedFileDetails.path.length > 0)
        },
        unselectable: {path: "", show: false}
    })

    /**
     *   Set some state information about the number of clicks the user has made. This is only
     *   set at the root of the tree and after that the state variables are passed as props to each
     *   branch. See the notes in the type definition for more information.
     */
    const [folderClickCountState, setFolderClickCountState] = useState<number>( 0)

    // Pick either the state or the prop version depending on whether we are in the root of the tree or a branch
    // Note that popup can not be undefined now
    const popup = popupProp || popupState
    const setPopup = setPopupProp || setPopupState

    const folderClickCount = folderClickCountProp !== undefined ? folderClickCountProp : folderClickCountState
    const setFolderClickCount = setFolderClickCountProp !== undefined ? setFolderClickCountProp : setFolderClickCountState

    return (

        <SubTree allowMultipleMenusOpen={allowMultipleMenusOpen} showUnselectableFiles={showUnselectableFiles}>

            {/*Iterate over each key in the fileTree, the key can be a folder or a file*/}
            {Object.entries(fileTree).map(([key, subFileTree], i) => {

                // If the key relates to an object that has a type property then that tells us
                // that the node is not a folder but a file.
                if (hasOwnProperty(subFileTree, "type") && typeof subFileTree.type === "string" && subFileTree.type !== "tree" && (subFileTree.selectable || showUnselectableFiles)  && typeof subFileTree.fileExtension === "string" && typeof subFileTree.path === "string" && typeof subFileTree.selectable=== "boolean" && typeof subFileTree.sha=== "string" && typeof subFileTree.size=== "number") {

                    return (
                        <File fileExtension={subFileTree.fileExtension}
                              folderClickCount={folderClickCount}
                              isTreeRoot={isTreeRoot}
                              key={i}
                              onSelectFile={onSelectFile}
                              path={subFileTree.path}
                              popup={popup}
                              selectable={subFileTree.selectable}
                              selectedFileDetails={selectedFileDetails}
                              setPopup={setPopup}
                              sha={subFileTree.sha}
                              showUnselectableFiles={showUnselectableFiles}
                              size={subFileTree.size}
                              title={key}
                        />
                    )

                } else if (hasOwnProperty(subFileTree, "type") && typeof subFileTree.type === "string" && subFileTree.type !== "tree") {

                    // An optimisation to not render files that are not shown to the user until needed
                    return (
                        <React.Fragment key={i}/>
                    )

                } else {

                    // This is the path to the folder, it starts off as undefined in the root of the file tree, then
                    // as we move down each branch we append on the key or the folder from the fileTree, These paths
                    // match with keys in the folderTree.
                    const newLocation = location === undefined ? key : `${location}/${key}`

                    // We have to identify folders, but we have to address folders that have a sub folder or a file called 'path'.
                    return (
                        <ExpandableFolder isExpandable={true}
                                          isTreeRoot={isTreeRoot}
                                          key={key}
                                          openOnLoad={Boolean(openFoldersOnLoad.includes(newLocation))}
                                          selectable={Boolean(folderTree[newLocation].selectable)}
                                          selectedFileInFolder={Boolean(popup && popup.selectable.path && popup.selectable.path.startsWith(newLocation))}
                                          setFolderClickCount={setFolderClickCount}
                                          showUnselectableFiles={showUnselectableFiles}
                                          title={key}
                                          unselectableFileInFolder={Boolean(popup && popup.unselectable.path && popup.unselectable.path.startsWith(newLocation))}
                        >
                            <FileTree {...props}
                                      popup={popup}
                                      fileTree={subFileTree as FileTreeType}
                                      folderClickCount={folderClickCount}
                                      folderTree={folderTree}
                                      setFolderClickCount={setFolderClickCount}
                                      setPopup={setPopup}
                                      isTreeRoot={false}
                                      location={newLocation}
                            />

                        </ExpandableFolder>
                    )
                }
            })}
        </SubTree>
    )
};

FileTreeInner.propTypes = {

    allowMultipleMenusOpen: PropTypes.bool.isRequired,
    fileTree: PropTypes.object.isRequired,
    folderTree: PropTypes.object.isRequired,
    isTreeRoot: PropTypes.bool,
    location: PropTypes.string,
    onSelectFile: PropTypes.func.isRequired,
    openFoldersOnLoad: PropTypes.arrayOf(PropTypes.string).isRequired,
    popup: PropTypes.shape({
        selectable: PropTypes.shape({
            path: PropTypes.string.isRequired,
            show: PropTypes.bool.isRequired
        }),
        unselectable: PropTypes.shape({
            path: PropTypes.string.isRequired,
            show: PropTypes.bool.isRequired
        })
    }),
    selectedFileDetails: PropTypes.shape({
        path: PropTypes.string.isRequired,
        selectable: PropTypes.bool,
        size: PropTypes.number,
        fileExtension: PropTypes.string
    }),
    setPopup: PropTypes.func,
    showUnselectableFiles: PropTypes.bool.isRequired
};

export const FileTree = memo(FileTreeInner);