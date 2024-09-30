/**
 * A dummy component used as a child component to the {@link ExpandableMenu} component. This is needed as we
 * pass props to this component and React does not allow props to be passed to a div. So for example
 * React does not allow <div isExpandable={true}/> as it will raise a warning.
 *
 * @module ExpandableHeader
 * @category Component
 */

import React, {memo} from "react";

/**
 * An interface for the props of the ExpandableHeader component.
 */
export interface Props {

    /**
     * Whether the child menu item is an expandable item or a link to a page.
     */
    isExpandable: boolean
    /**
     * The key for the child menu item.
     */
    key: string
    /**
     * Whether the child menu item, if it is expandable, should be open when the menu mounts.
     */
    openOnLoad?: boolean
    /**
     * The title to show for the menu item.
     */
    title: string
}

const ExpandableHeader = (props: React.PropsWithChildren<Props>) => {

    return (<React.Fragment>{props.children}</React.Fragment>)
};

export default memo(ExpandableHeader, () => true);