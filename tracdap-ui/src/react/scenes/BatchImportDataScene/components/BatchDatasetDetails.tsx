/**
 * A component that shows a table of information about a batch import dataset definition.
 * @module
 * @category BatchDataImportScene component
 */

import {Badges} from "../../../components/Badges";
import {convertKeyToText} from "../../../utils/utils_string";
import {Icon} from "../../../components/Icon";
import {isPrimitive} from "../../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";
import {UiBatchImportDataRow} from "../../../../types/types_general";
import {useAppSelector} from "../../../../types/types_hooks";

// Additional tooltips for some fields in the dataset
const tooltips: Record<string, string> = {
    RECONCILIATION_FIELDS: "This information will be created and checked to ensure no data is lost in the transfer",
    RECONCILIATION_ITEM_SUFFIX: "Any files with this in their path will be excluded"
}

/**
 * An interface for the props of the BatchDatasetDetails component.
 */
export interface Props {

    /**
     * The definition for the batch data import.
     */
    batchImportDefinition?: UiBatchImportDataRow
}

export const BatchDatasetDetails = (props: Props) => {

    const {batchImportDefinition} = props

    // Get what we need from the store
    const {fields} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_batch_import_data.currentDefinition)

    // Fields that we need some additional logic to show to the user
    const specialTreatmentFields = ["RECONCILIATION_FIELDS", "BUSINESS_SEGMENTS"]

    return (
        <React.Fragment>
            {batchImportDefinition !== undefined &&
                <Table bordered={true}
                       className={`fs-13 mt-3 mb-2 table-no-outline`}
                       size={"sm"}
                       striped={true}
                >
                    <tbody>
                    {Object.entries(batchImportDefinition).map(([key, value]) =>

                        <tr key={key}>
                            <td className={"text-nowrap align-top pe-2"}>
                                {fields.find(fieldSchema => fieldSchema.fieldName === key)?.label || convertKeyToText(key)}
                                {tooltips.hasOwnProperty(key) &&
                                    <Icon ariaLabel={"More information"}
                                          className={"ms-2"}
                                          icon={"bi-question-circle"}
                                          tooltip={tooltips[key]}
                                    />}
                                {" "}:
                            </td>

                            <td>
                                {/*It is less efficient but far more readable to execute as a series of ifs*/}
                                {value == null && "-"}
                                {value !== null && isPrimitive(value) && !specialTreatmentFields.includes(key) && <span className={"user-select-all"}>{value.toString()}</span>}
                                {typeof value === "string" && key === "RECONCILIATION_FIELDS" &&

                                    <ul className={"m-0 pl-3"}>
                                        {value.split(":").map((field, i) =>
                                            <li key={i}>{field}</li>
                                        )}
                                    </ul>
                                }
                                {typeof value === "string" && key === "BUSINESS_SEGMENTS" &&

                                    // TODO this should be enriched
                                    <Badges convertText={false}
                                            delimiter={"||"}
                                            text={value}/>
                                }
                            </td>
                        </tr>
                    )}
                    </tbody>
                </Table>
            }
        </React.Fragment>
    )
};

BatchDatasetDetails.propTypes = {

    batchImportDefinition: PropTypes.object
};