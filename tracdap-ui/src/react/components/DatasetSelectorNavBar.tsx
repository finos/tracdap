/**
 * A component that shows a dropdown menu for the input datasets for a flow or input/output datasets from a job.
 * When one is selected a function is triggered.
 *
 * @module DatasetSelectorNavBar
 * @category Component
 */

import {getObjectName} from "../utils/utils_trac_metadata";
import {isTagOption} from "../utils/utils_trac_type_chckers";
import Nav from "react-bootstrap/Nav";
import NavDropdown from "react-bootstrap/NavDropdown";
import PropTypes from "prop-types";
import React, {memo} from "react";
import type {SearchOption} from "../../types/types_general";
import type {SingleValue} from "react-select";
import {sortArrayBy} from "../utils/utils_arrays";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the DatasetSelectorNavBar component.
 */
export interface Props {

    /**
     * The metadata tags for the datasets to show in the menu, keyed by the key used in the job.
     */
    datasets?: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>,
        /**
     * Whether the menu should be disabled, for example while a request is running.
     */
    disabled: boolean
    /**
     * A function that gets the data when the dataset to show is changed.
     */
    handleGetData: (key: string | null) => void,
    /**
     * The metadata tag for the selected dataset.
     */
    tag?: trac.metadata.ITag
}

const DatasetSelectorNavBarInner = (props: Props) => {

    const {datasets, disabled, handleGetData, tag} = props

    // Make the list appear in  alphabetical order, remove any entries that do not have a tag associated with them
    // This can happen for optional inputs that have no object selected
    const items = datasets && sortArrayBy(Object.entries(datasets).filter(([key, item]) => {

        // Remove null options
        return item != null

    }).map(([key, item]) => ({key, label: getObjectName(isTagOption(item) ? item.tag : item, false, false, false, false)})), "label") || []

    return (
        <React.Fragment>
            {datasets && items.length > 0 &&
                <Nav onSelect={handleGetData}>
                    <NavDropdown
                        title={!tag ? "Please select" : getObjectName(tag, false, false, false, false)}
                        id={"collapsible-nav-dropdown"}
                        disabled={disabled}
                    >
                        {items.map(({key, label}) =>

                            <NavDropdown.Item id={key} key={key} className={"fs-13"} eventKey={key}>
                                {label}
                            </NavDropdown.Item>
                        )}
                    </NavDropdown>
                </Nav>
            }
        </React.Fragment>
    )
}

DatasetSelectorNavBarInner.propTypes = {

    datasets: PropTypes.object,
    disabled: PropTypes.bool.isRequired,
    handleGetData: PropTypes.func.isRequired,
    tag: PropTypes.object,
};

export const DatasetSelectorNavBar = memo(DatasetSelectorNavBarInner)