/**
 * A component that allows the user to set the order of a field in the schema editor.
 *
 * @module FieldOrder
 * @category Component
 */

import {Button} from "../Button";
import type {ButtonPayload} from "../../../types/types_general";
import {Icon} from "../Icon";
import {LabelLayout} from "../LabelLayout";
import PropTypes from "prop-types";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the FieldOrder component.
 */
export interface Props {

    /**
     * Whether the fieldName can be edited. When creating a schema you want to be able to edit it but not when
     * editing the schema of a dataset loaded by CSV or Excel.
     */
    canEditFieldName: boolean
    /**
     * The name of the field in the schema.
     */
    fieldName: trac.metadata.IFieldSchema["fieldName"]
    /**
     * The order of the field in the schema.
     */
    fieldOrder: trac.metadata.IFieldSchema["fieldOrder"]
    /**
     * The position of the field in the schema.
     */
    index: number
    /**
     * The function to run when the field schema is changed. This updates the edited property in the parent.
     * This will either be in a store or a state of the parent component.
     */
    onChange: (payload: ButtonPayload) => void
    /**
     * The number of fields in the schema, this is used to disable some buttons.
     */
    schemaLength: number
}

export const FieldOrder = (props: Props) => {

    const {
        canEditFieldName,
        fieldName,
        fieldOrder,
        index,
        onChange,
        schemaLength
    } = props

    return (

        <div className={"d-flex justify-content-center align-items-end flex-shrink-0 px-2"}>
            <LabelLayout labelText={canEditFieldName ? "Order:" : undefined} labelPosition={"top"} className={"mb-0 text-center"}>
                <div className={"d-flex justify-content-center"}>

                    <Button ariaLabel={"Move field up"}
                            className={`m-0 p-0 fs-3`}
                            id={fieldName || undefined}
                            index={index}
                            name={"up"}
                            variant={"link"}
                            disabled={fieldOrder === 0}
                            onClick={onChange}
                            isDispatched={false}
                    >
                        <Icon ariaLabel={false}
                              icon={"bi-arrow-up"}
                              tooltip={"Move up"}
                        />
                    </Button>

                    <div className={`${fieldOrder == null ? "mx-0" : "mx-1"} text-center my-auto fs-8`}>
                        {fieldOrder == null ? <React.Fragment>
                            <div>not</div>
                            <div>set</div>
                        </React.Fragment> : fieldOrder + 1}
                    </div>

                    <Button ariaLabel={"Move field down"}
                            className={"m-0 p-0 fs-3"}
                            id={fieldName || undefined}
                            index={index}
                            name={"down"}
                            variant={"link"}
                            disabled={fieldOrder === schemaLength - 1}
                            onClick={onChange}
                            isDispatched={false}
                    >
                        <Icon ariaLabel={false}
                              icon={"bi-arrow-down"}
                              tooltip={"Move down"}
                        />
                    </Button>

                </div>
            </LabelLayout>
        </div>
    )
}

FieldOrder.propTypes = {

    canEditFieldName: PropTypes.bool.isRequired,
    fieldName: PropTypes.string.isRequired,
    fieldOrder: PropTypes.number,
    index: PropTypes.number.isRequired,
    onChange: PropTypes.func.isRequired,
    schemaLength: PropTypes.number.isRequired
};