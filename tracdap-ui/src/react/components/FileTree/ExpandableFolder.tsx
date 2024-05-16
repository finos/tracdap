/**
 * A dummy component used as a child to the {@link FileTree} component. This is needed as we need
 * to pass props to the children but React does not allow props to be passed to a div. So for example
 * React does not allow <div isExpandable={true}/> as it will raise a warning.
 *
 * @module ExpandableFolder
 * @category Component
 */

import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * A function that runs to see if the ExpandableFolder component should rerender. This is an optimisation to the
 * performance of this component.
 */
const isEqualExpandableFolder = (prevProps: Props, nextProps: Props): boolean => (

    !(prevProps.showUnselectableFiles !== nextProps.showUnselectableFiles || nextProps.selectedFileInFolder || prevProps.selectedFileInFolder || nextProps.unselectableFileInFolder || prevProps.unselectableFileInFolder)
)

/**
 * An interface for the props of the ExpandableFolder component.
 */
export interface Props {

    /**
     * Whether the item in the tree is expandable, if it is then the SubTree component will show the item as a folder,
     * otherwise it will be shown as a file.
     */
    isExpandable: boolean
    /**
     * Whether the branch being rendered is the first node in the tree or a latter one, this allows us to indent
     * branches for each tier but have the first tier unindented. This prop is false at the root of the tree.
     */
    isTreeRoot: boolean
    /**
     * Whether the item, if it is a folder, should be shown as open when the component mounts. This is used when the
     * user has opened a folder, selected a file and then closed the component - then they go back to it. In this case
     * we want the component show as it was before it was closed.
     */
    openOnLoad: boolean
    /**
     * Whether the file or folder is selectable, unselectable items have different icons. This prop is used to set the
     * icon and also the visibility of the item (unselectable items can be hidden).
     */
    selectable: boolean
    /**
     * Whether the folder sits within the last file that was clicked on that was selectable. So for example if the
     * file was 'impairment/models/my_code.py' and we were rendering the 'models' folder this flag would be true. This
     * prop is not used in the rendering, instead it is used in optimising which parts of the file tree re-render when
     * the selected file changes (see the isEqualExpandableFolder function).
     */
    selectedFileInFolder: boolean
    /**
     * A function to update the values associated with the number of folder clicks done by the user.
     * This prop should not be set on the root FileTree component but is passed down internally when the component
     * is recursively used to show branches. This sets a state in the root of the FileTree component that is shared
     * to all branches. This prop is used to redraw pop up messages in the user interface when their position should
     * change.
     */
    setFolderClickCount: React.Dispatch<React.SetStateAction<number>>
    /**
     * Whether to show folders and files labelled as hidden/unselectable. These are empty folders for
     * example or files that are not eligible for some process further down such as loading as a
     * dataset.
     */
    showUnselectableFiles: boolean
    /**
     * The name of the file or folder.
     */
    title: string
    /**
     * Whether the folder sits within the last file that was clicked on that was unselectable. So for example if the
     * file was 'impairment/models/data.csv' and we were rendering the 'models' folder this flag would be true. This
     * prop is not used in the rendering, instead it is used in optimising which parts of the file tree re-render when
     * the unselected file changes (see the isEqualExpandableFolder function).
     */
    unselectableFileInFolder: boolean
}

const ExpandableFolder = (props: React.PropsWithChildren<Props>) => <React.Fragment>{props.children}</React.Fragment>

ExpandableFolder.propTypes = {
    children: PropTypes.oneOfType([PropTypes.element, PropTypes.arrayOf(PropTypes.element)]),
    isExpandable: PropTypes.bool.isRequired,
    isTreeRoot: PropTypes.bool.isRequired,
    openOnLoad: PropTypes.bool.isRequired,
    selectable: PropTypes.bool.isRequired,
    selectedFileInFolder: PropTypes.bool.isRequired,
    setFolderClickCount: PropTypes.func.isRequired,
    showUnselectableFiles: PropTypes.bool.isRequired,
    title: PropTypes.string.isRequired,
    unselectableFileInFolder: PropTypes.bool.isRequired
};

export default memo(ExpandableFolder, isEqualExpandableFolder);