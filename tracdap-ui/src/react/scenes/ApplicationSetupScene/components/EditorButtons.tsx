/**
 * A component that shows a set of buttons in the editor that allows the user to add to, reset or save their changes.
 * @module
 * @category Component
 */

import {addRowToData, resetEditor, toggleValidationMessages, updateSetupItem,} from "../store/applicationSetupStore";
import {Alert} from "../../../components/Alert";
import {Button} from "../../../components/Button";
import {ConfirmButton} from "../../../components/ConfirmButton";
import Col from "react-bootstrap/Col";
import {Icon} from "../../../components/Icon";
import {objectsContainsValue} from "../../../utils/utils_object";
import React from "react";
import Row from "react-bootstrap/Row";
import {showToast} from "../../../utils/utils_general";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {makeArrayOfObjectsUnique} from "../../../utils/utils_arrays";

/**
 * A function that returns a human-readable name for the user interface dataset being edited, this is for button labels etc.
 * @param datasetKey - The key from the store to make readable.
 */
function setReadableText(datasetKey: string) {

    return datasetKey === "ui_attributes_list" ? "attribute" : datasetKey === "ui_parameters_list" ? "parameter" : datasetKey === "ui_batch_import_data" ? "batch import" : "business segment"
}

export const EditorButtons = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {editor: {control: {key: datasetKey, userChangedSomething}}, editor} = useAppSelector(state => state["applicationSetupStore"])

    /**
     * A function that acts as a wrapper to the 'save' button, it performs a check before making the TRAC
     * API request to save the edited dataset in TRAC.
     */
    const handleSave = (): void => {

        if (datasetKey === null) return
        const humanReadableKey = setReadableText(datasetKey)

        // Did the user actually change anything
        if (!userChangedSomething) {

            showToast("warning", "There were no changes detected so nothing was saved.", "no-changes")

        } else if (objectsContainsValue(editor.items[datasetKey].validation.isValid, false)) {

            dispatch(toggleValidationMessages(true))
            showToast("error", `There are problems with your ${humanReadableKey}, please fix the issues shown and try again.`, "handleSave/rejected")

        } else if (makeArrayOfObjectsUnique(editor.items[datasetKey].data).length !== editor.items[datasetKey].data.length) {

            showToast("warning", `You have duplicated ${humanReadableKey}s, please delete or edit a duplicate and try again.`, "no-changes")

        } else {

            dispatch(updateSetupItem())
        }
    };

    return (
        <React.Fragment>

            {datasetKey !== null && editor.items[datasetKey].data.length === 0 &&
                <Row>
                    <Col xs={12} lg={{offset: 2, span: 8}}>
                        <Alert className={"text-center my-3 py-3"} variant={"success"}>
                            <span className={"d-flex"}>
                                There are no {setReadableText(datasetKey)} set, please <Button ariaLabel={`Add ${setReadableText(datasetKey)}`} className={"my-0 mx-1 p-0"} isDispatched={true}
                                                                                               onClick={addRowToData} variant={"link"}>click here</Button> to add one.
                            </span>
                        </Alert>
                    </Col>
                </Row>
            }

            {/*We hide the editor buttons if the editor is not being used and also if there are no rows. This is*/}
            {/*purely a cosmetic choice.*/}
            {datasetKey !== null && !(editor.items[datasetKey].data.length === 0) &&
                <Row className={"mt-5 pt-2"}>
                    <Col>
                        <ConfirmButton ariaLabel={"Save changes"}
                                       className={"min-width-px-120 float-end"}
                                       description={"Are you sure you want to save your changes in TRAC, this will affect all users of the tenant?"}
                                       disabled={!userChangedSomething}
                                       ignore={!userChangedSomething}
                                       onClick={handleSave}
                        >
                            <Icon icon={"bi-save"}
                                  ariaLabel={false}
                                  className={"me-2"}
                            />Save
                        </ConfirmButton>

                        <ConfirmButton ariaLabel={"Reset changes"}
                                       className={"me-2 min-width-px-120 float-end"}
                                       description={"All unsaved changes will be lost do you want to continue?"}
                                       disabled={!userChangedSomething}
                                       ignore={!userChangedSomething}
                                       dispatchedOnClick={resetEditor}
                        >
                            <Icon icon={"bi-arrow-counterclockwise"}
                                  ariaLabel={false}
                                  className={"me-2"}
                            />
                            Reset
                        </ConfirmButton>

                        <Button ariaLabel={"Add a row"}
                                className={"me-2 min-width-px-120 float-end"}
                                isDispatched={true}
                                onClick={addRowToData}
                        >
                            <Icon ariaLabel={false}
                                  className={"me-2"}
                                  icon={"bi-plus-lg"}
                            />{`Add ${setReadableText(datasetKey)}`}
                        </Button>
                    </Col>
                </Row>
            }
        </React.Fragment>
    )
};