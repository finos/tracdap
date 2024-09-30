/**
 * This component that shows a list of what datasets are needed in TRAC for the application to work, whether they
 * have been found and buttons to allow them to be added/edited.
 * @module
 * @category Component
 */

import {addDatasetToEditor, createSetupItem} from "../store/applicationSetupStore";
import {ButtonToDownloadData} from "../../../components/ButtonToDownloadData";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {getObjectName} from "../../../utils/utils_trac_metadata";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Icon} from "../../../components/Icon";
import React from "react";
import Table from "react-bootstrap/Table";
import {useAppSelector} from "../../../../types/types_hooks";

export const DatasetSummaryTable = () => {

    // Get what we need from the store
    const {items} = useAppSelector(state => state["applicationSetupStore"].tracItems)
    const {key: keyOfEditedDataset, userChangedSomething} = useAppSelector(state => state["applicationSetupStore"].editor.control)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    return (

        <React.Fragment>

            <HeaderTitle type={"h3"} outerClassName={"mt-4 pb-3"} text={"Summary of required datasets"}/>

            <Table className={"dataHtmlTable fs-7 my-3"} responsive>

                <thead>
                <tr>
                    <th className={"text-nowrap min-width-px-0"}>Name</th>
                    <th className={"text-nowrap d-none d-md-table-cell"}>Description</th>
                    <th className={"text-center text-nowrap"}>Dataset found</th>
                    <th className={"text-center text-nowrap"}>Update</th>
                    <th className={"text-center text-nowrap"}>Download</th>
                </tr>
                </thead>

                <tbody>

                {(Object.keys(items) as Array<keyof typeof items>).map((key) => {

                    // Destructure to make render code shorter
                    const {currentDefinition, defaultDefinition} = items[key]

                    return (
                        <tr key={key}>
                            <td className={"text-nowrap py-3 pe-4 align-text-top"}>
                                {/*Use the name attribute of the dataset found in TRAC or the default name if that is not available*/}
                                {currentDefinition.tag?.attrs ? getObjectName(currentDefinition.tag, false, false, false, false) : defaultDefinition.attrs.find(attr => attr.attrName === "name")?.value?.stringValue || "Unknown"}
                            </td>

                            <td className={"py-3 d-none d-md-table-cell"}>
                                {/*Use the description attribute of the dataset found in TRAC or the default name if that is not available*/}
                                {`${currentDefinition.tag?.attrs?.description?.stringValue || items[key].defaultDefinition.attrs.find(attr => attr.attrName === "description")?.value?.stringValue || undefined}`}
                            </td>

                            <td className={"text-center align-middle"}>
                                <Icon size={"2rem"}
                                      icon={currentDefinition.foundInTrac ? "bi-check-circle" : "bi-x-circle"}
                                      className={currentDefinition.foundInTrac ? "text-success" : "text-danger"}
                                      ariaLabel={currentDefinition.foundInTrac ? "Data found" : "Data not found"}
                                      colour={null}
                                />
                            </td>

                            <td className={"text-center align-middle"}>

                                {/* Note we do not allow the creation of datasets if we are looking in the past*/}
                                <ConfirmButton ariaLabel={currentDefinition.foundInTrac ? "Edit" : "Create"}
                                               description={key === keyOfEditedDataset ? "Resetting will lose all unsaved changes, do you want to continue?" : "Changing dataset will mean all unsaved changes to the one you were editing will be lost, do you want to continue?"}
                                               disabled={Boolean(searchAsOf || (currentDefinition.foundInTrac && key === keyOfEditedDataset) && !userChangedSomething)}
                                               dispatchedOnClick={currentDefinition.foundInTrac ? addDatasetToEditor : createSetupItem}
                                               id={key}
                                               ignore={!userChangedSomething}
                                               name={currentDefinition.foundInTrac ? "edit" : "create"}
                                               variant={"info"}
                                >
                                    {(currentDefinition.foundInTrac && key !== keyOfEditedDataset) ? "Edit" : (currentDefinition.foundInTrac && key === keyOfEditedDataset) ? "Reset" : "Create"}
                                </ConfirmButton>

                            </td>

                            <td className={"text-center align-middle"}>
                                {currentDefinition.foundInTrac &&
                                    <ButtonToDownloadData tag={currentDefinition.tag}/>
                                }
                                {!currentDefinition.foundInTrac && "-"}
                            </td>
                        </tr>
                    )
                })}
                </tbody>
            </Table>
        </React.Fragment>
    )
};