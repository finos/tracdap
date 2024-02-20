/**
 * A component that shows the user the editor page for the dataset that they have chosen to edit. Moving the selection
 * of the component to show to a separate component is a render optimisation.
 * @module
 * @category Component
 */

import {AttributeListEditor} from "./AttributeListEditor";
import {BusinessSegmentOptionsEditor} from "./BusinessSegmentOptionsEditor";
import {capitaliseString} from "../../../utils/utils_string";
import {EditorButtons} from "./EditorButtons";
import {HeaderTitle} from "../../../components/HeaderTitle";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * A function that returns a human-readable name for the user interface dataset being edited, this is for button labels etc.
 * @param datasetKey - The key from the store to make readable.
 */
function setReadableText(datasetKey: string) {

    return datasetKey === "ui_attributes_list" ? "attribute" : datasetKey === "ui_parameters_list" ? "parameter" : datasetKey === "ui_batch_import_data" ? "batch import" : "business segment"
}

export const Editor = () => {

    console.log("Rendering Editor")

    // Get what we need from the store
    const {status} = useAppSelector(state => state["applicationSetupStore"].tracItems.getSetupItems)
    const {show, key: keyOfEditedDataset} = useAppSelector(state => state["applicationSetupStore"].editor.control)

    return (

        <React.Fragment>

            {status === "succeeded" && show && keyOfEditedDataset != null &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} outerClassName={"mt-4 pb-3"} text={`${capitaliseString(setReadableText(keyOfEditedDataset))} editor`}/>

                    {keyOfEditedDataset === "ui_attributes_list" &&
                        <AttributeListEditor/>
                    }

                    {keyOfEditedDataset === "ui_business_segment_options" &&
                        <BusinessSegmentOptionsEditor/>
                    }

                    <EditorButtons/>
                </React.Fragment>
            }

        </React.Fragment>
    )
};