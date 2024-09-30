/**
 * A component that shows button that when clicked on redirects the user to the correct scene to be able to view the
 * selected object from their search. It also adds the required tagSelector for thr object to the objectSummaryStore
 * so that the metadata can be downloaded.
 *
 * @module ViewItemButton
 * @category Component
 */

import {Button} from "../Button";
import {convertObjectTypeToString} from "../../utils/utils_trac_metadata";
import {type FindInTracStoreState} from "./findInTracStore";
import {Icon} from "../Icon";
import {isTracObjectType} from "../../utils/utils_trac_type_chckers";
import {ObjectSummaryPaths} from "../../../config/config_menu";
import PropTypes from "prop-types";
import React from "react";
import {useAppSelector} from "../../../types/types_hooks";
import {useNavigate} from "react-router-dom";

/**
 * An interface for the props of the ViewItemButton component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

export const ViewItemButton = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {[storeKey]: storeUser, [storeKey]: {selectedTab}} = useAppSelector(state => state["findInTracStore"].uses)

    // A hook from the React Router plugin that allows us to navigate using onClick events, in this case we move to a
    // page that shows information about the selected object from the table
    const navigate = useNavigate()

    // The selected rows
    const selectedRows = storeUser.selectedResults[selectedTab];

    /**
     * A function that runs when the user clicks to view an object that they have selected from their search results,
     * it checks that the right information is available and if so it puts the tagSelector in the objectSummaryStore
     * which will trigger an API to get the full tag and then navigates to the right page to view the information.
     */
    const handleButtonClick = () => {

        if (selectedRows.length > 0) {

            if (!isTracObjectType(selectedRows[0].objectType) || typeof selectedRows[0].objectId !== "string" || typeof selectedRows[0].objectVersion !== "number" || typeof selectedRows[0].tagVersion !== "number") {
                throw new Error("The selected row does not have all of the information required to view the object")
            }

            navigate(`${ObjectSummaryPaths[selectedRows[0].objectType].to}/${selectedRows[0].objectId}/${selectedRows[0].objectVersion}/${selectedRows[0].tagVersion}`)
        }
    }

    return (

        <Button ariaLabel={"View "}
                className={"min-width-px-150 float-end"}
                disabled={Boolean(selectedRows.length < 1 || storeUser[selectedTab].status !== "succeeded")}
                isDispatched={false}
                onClick={handleButtonClick}
        >
            <Icon ariaLabel={"false"}
                   className={"me-2"}
                   icon={"bi-binoculars"}
            />
            View {convertObjectTypeToString(storeUser[selectedTab].objectTypeResultsAreFor, true, true)}
        </Button>
    )
};

ViewItemButton.propTypes = {

    storeKey: PropTypes.string.isRequired,
};