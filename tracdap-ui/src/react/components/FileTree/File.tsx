/**
 * A component that shows a file icon and the file name in the tree. This also creates an overlay or popup
 * so that when a file name is clicked on it causes the pop-up to show. There are two formats of popup.
 * First, where it is for a file that can be legitimately selected in which case the selected file is stored
 * using the onSelectFile function and the popup persists in the UI and second, where the file is ineligible
 * for selection in which has the popup auto closes.
 *
 * @module File
 * @category Component
 */

import {Icon} from "../Icon";
import Overlay from "react-bootstrap/Overlay";
import Popover from "react-bootstrap/Popover";
import type {PopupDetails, SelectedFileDetails} from "../../../types/types_general";
import PropTypes from "prop-types";
import {setLanguageOrFileIcon} from "../../utils/utils_general";
import React, {memo, useEffect, useRef} from "react";
import {humanReadableFileSize} from "../../utils/utils_number";

// A config for the Overlay component that sets a margin for the popup. The negative first offset moves it up, the
// arrow still points to the selected file, but it is forced down relative to the box so that it is aligned only
// with the body and not at the join between the header and the body. The second offset moves the popover to the right
const popperConfig = {
    modifiers: [{
        name: 'offset',
        options: {
            offset: [-10, 20]
        },
    }]
}

/**
 * A function that runs to see if the File component should rerender. This is an optimisation to the
 * performance of this component.
 */
const isEqualFile = (prevProps: Props, nextProps: Props): boolean => {

    return (
        !(
            // The selected file is changed and the file was or is this one
            ((prevProps.selectedFileDetails != null && nextProps.selectedFileDetails == null && nextProps.path === prevProps.selectedFileDetails.path) || (prevProps.selectedFileDetails == null && nextProps.selectedFileDetails != null && nextProps.path === nextProps.selectedFileDetails.path)) ||
            // The user toggles to show/hide the unselectable items and the file is not selectable (so will be affected) or is the file selected (the Overlay will need to be redrawn)
            (prevProps.showUnselectableFiles !== nextProps.showUnselectableFiles && (!nextProps.selectable || (nextProps.selectedFileDetails != null && nextProps.path === nextProps.selectedFileDetails.path))) ||
            // The file is selectable and the popup is shown/hidden and the selected file changes and this file is/was selected
            (nextProps.selectable && (prevProps.popup.selectable.show !== nextProps.popup.selectable.show || (prevProps.selectedFileDetails != null && nextProps.selectedFileDetails != null && prevProps.selectedFileDetails.path !== nextProps.selectedFileDetails.path)) && (prevProps.selectedFileDetails != null && nextProps.selectedFileDetails != null && (prevProps.path === prevProps.selectedFileDetails.path || nextProps.path === nextProps.selectedFileDetails.path))) ||
            // The file is not selectable and unselectable file clicked on was changed
            (!prevProps.selectable && (prevProps.path === prevProps.popup.unselectable.path || nextProps.path === nextProps.popup.unselectable.path)) ||
            // A folder was opened or closed, there is a file selected and this file is selectable - reposition the popup
            (prevProps.folderClickCount !== nextProps.folderClickCount && nextProps.selectedFileDetails != null && nextProps.path === nextProps.selectedFileDetails.path)
        )
    )
}

/**
 * An interface for the props of the File component.
 */
export interface Props {

    /**
     * The file extension for the file, e.g. 'py'.
     */
    fileExtension: string
    /**
     * A count for the number of clicks on folders made by the user, we do this as part of optimising the rendering of
     * this component. When someone clicks on a file a popup is shown next to it to indicate that it has been selected.
     * However, when the user then licks on a folder the position of the file icon and label moves but popup stays where
     * it is. This is because we have memoized the components to only re-render on certain events. So we add a state
     * variable that is the count of user clicks on folders, and we will add a rule that says if a file is selected
     * and the folder click count changes then the popup should be redrawn, so it moves with the file icon and label.
     */
    folderClickCount: number
    /**
     * Whether the branch being rendered is the first node in the tree or a latter one, this allows us to indent
     * branches for each tier but have the first tier unindented. This prop is false at the root of the tree. Note that
     * this prop is not used by this component. This prop is 'captured' by the SubTree component and used when it
     * renders the children components, including this component.
     */
    isTreeRoot: boolean
    /**.
     * A function that runs when the user clicks on a file in the tree, this passes back
     * information about the selected file to be passed to a parent component
     */
    onSelectFile: React.Dispatch<React.SetStateAction<null | SelectedFileDetails>>
    /**
     * The path to the file, including the file at the end.
     */
    path: string
    /**
     * The state of the popup messages.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to only enable a single pop up message in the user interface.
     */
    popup: PopupDetails
    /**
     * Whether the file is selectable.
     */
    selectable: boolean
    /**
     * The details of the selected file. This is the path property of the file and is a unique reference
     * to the selected file (the file name is may not be unique across the tree). This is used to
     * select which PopOver overlay to show.
     */
    selectedFileDetails: null | SelectedFileDetails
    /**
     * A function to update the values associated with the popups.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to only enable a single pop up message in the user interface.
     */
    setPopup: React.Dispatch<React.SetStateAction<PopupDetails>>
    /**
     * The SHA for the file (not the commit).
     */
    sha: string
    /**
     * Whether to show folders and files labelled as hidden/unselectable. These are empty folders for
     * example or files that are not eligible for some process further down such as loading as a
     * dataset.
     */
    showUnselectableFiles: boolean
    /**
     * The size of the model, in bytes.
     */
    size: number
    /**
     * The name of the file to show in the file tree.
     */
    title: string
}

const FileInner = (props: Props) => {

    console.log("File")

    const {
        fileExtension,
        folderClickCount,
        onSelectFile,
        path,
        popup,
        selectable,
        setPopup,
        sha,
        showUnselectableFiles,
        size,
        title
    } = props

    const refUseRef = useRef(null)

    /**
     * A function that runs when the user clicks on a file name.
     */
    const handleClick = () => {

        // If the file is ineligible for selection show the overlay
        if (!selectable) {

            setPopup(prevState => ({
                selectable: {path: prevState.selectable.path, show: false},
                unselectable: {path: path, show: true}
            }))
        }

        //  We only update the store if the item is selectable, selections can be toggled.
        if (selectable) {

            // If not selected already
            if (path !== popup.selectable.path) {
                setPopup({selectable: {path, show: true}, unselectable: {path: "", show: false}})
                onSelectFile({fileExtension, path, selectable, sha, size})
            } else {
                // If selected already
                setPopup({selectable: {path: "", show: false}, unselectable: {path: "", show: false}})
                onSelectFile(null)
            }
        }
    }

    // A custom hook that is run to hide popups associated
    // with files that are ineligible for selection
    // see https://overreacted.io/making-setinterval-declarative-with-react-hooks/
    function useInterval(callback1: () => void, callback2: () => void, delay: number, popup: PopupDetails, selectable: boolean) {

        // Create a function that is stored in a ref (changing it does not cause a rerender)
        const savedCallback1 = useRef<() => void>();
        const savedCallback2 = useRef<() => void>();

        // Remember the two arguments in the refs. These are set state functions passed to the File component as a prop.
        useEffect(() => {
            savedCallback1.current = callback1;
        }, [callback1]);

        useEffect(() => {
            savedCallback2.current = callback2;
        }, [callback2]);

        // Set up the interval.
        useEffect(() => {

            function tick() {
                if (popup.unselectable.show && !selectable) {
                    // Hide the popup
                    if (savedCallback1.current) savedCallback1.current();

                    // Only restore the selected file popup if it remains hidden to, this is because additional
                    // selections may have been set by the user.
                    if (!popup.selectable.show) {
                        // Show the popup
                        if (savedCallback2.current) savedCallback2.current();
                    }
                }
            }

            if (delay !== null) {
                let id = setInterval(tick, delay);
                return () => clearInterval(id);
            }

        }, [delay, selectable, popup]);
    }

    // Run the hook to show and then hide the "This file is not selectable' popover
    useInterval(() => {
        setPopup((prevState) => ({
            selectable: {path: prevState.selectable.path, show: false},
            unselectable: {path: "", show: false}
        }))
    }, () => {
        setPopup(prevState => ({
            selectable: {path: prevState.selectable.path, show: true},
            unselectable: {path: "", show: false}
        }))
    }, 2000, popup, selectable);

    return (
        <React.Fragment>
            <div ref={refUseRef} className={"d-inline-block"}>
                <div className={`py-2 fs-9 d-flex align-items-center`}>
                    <Icon ariaLabel={"File"}
                           className={"me-3 pointer"}
                           icon={selectable ? setLanguageOrFileIcon(fileExtension) : "bi-file-earmark-text-fill"}
                    />
                    <span onClick={handleClick}
                          className={`pointer fs-8 ${(path === popup.selectable.path) && "fw-bold"}`}
                    >
                        {title}
                    </span>
                </div>
            </div>

            {/*Don't mount a hidden div for every folder, mount them when needed*/}
            {Boolean((refUseRef.current && popup.unselectable.show && path === popup.unselectable.path) || (popup.selectable.show && path === popup.selectable.path)) &&
                <Overlay show={true}
                    // The ref of the div to show the popover next to
                         target={refUseRef}
                         placement="right"
                    // The container means that the Popover will inherit the hiding classes of the parent div when the folder is closed
                         container={refUseRef}
                    // Give the Popover an extra margin
                         popperConfig={popperConfig}
                    // Key is a string that if it changes forces the Overlay component to remount, we do this
                    // when there might be an update that requires the popup to be repositioned
                         key={`${showUnselectableFiles.toString()}-${folderClickCount}`}
                >
                    <Popover id="popover">
                        <Popover.Header>{title} ({humanReadableFileSize(size)})</Popover.Header>
                        {selectable &&
                            <Popover.Body>
                                <div className={"fs-7 d-flex align-items-center"}>
                                    <Icon ariaLabel={"File selected"}
                                           className={"text-success me-2 align-middle"}
                                           icon={"bi-check-circle"}
                                    />
                                    <span>Selected</span>
                                </div>
                            </Popover.Body>
                        }
                        {!selectable &&
                            <Popover.Body>
                                <div className={"fs-7 d-flex align-items-center"}>
                                    <Icon ariaLabel={"File can't be selected"}
                                           icon={"bi-exclamation-diamond"}
                                           className={"text-warning me-2 align-middle"}
                                    />
                                    <span>File type can not be loaded</span>
                                </div>
                            </Popover.Body>
                        }
                    </Popover>
                </Overlay>
            }
        </React.Fragment>
    )
};

FileInner.propTypes = {

    fileExtension: PropTypes.string.isRequired,
    folderClickCount: PropTypes.number.isRequired,
    onSelectFile: PropTypes.func.isRequired,
    path: PropTypes.string.isRequired,
    popup: PropTypes.shape({
        selectable: PropTypes.shape({
            path: PropTypes.string.isRequired,
            show: PropTypes.bool.isRequired
        }),
        selectedFileDetails: PropTypes.object,
        unselectable: PropTypes.shape({
            path: PropTypes.string.isRequired,
            show: PropTypes.bool.isRequired
        })
    }).isRequired,
    selectable: PropTypes.bool.isRequired,
    setPopup: PropTypes.func.isRequired,
    sha: PropTypes.string.isRequired,
    showUnselectableFiles: PropTypes.bool.isRequired,
    size: PropTypes.number.isRequired,
    title: PropTypes.string.isRequired
};

export const File = memo(FileInner, isEqualFile);