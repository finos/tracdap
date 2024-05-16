/**
 * A component that shows a header row in the SchemaEditor, this is a set of divs to show at the top.
 *
 * @module SchemaHeader
 * @category Component
 */

import {Icon} from "../Icon";
import React, {memo} from "react";

const SchemaHeaderInner = () => {

    return (
        <div className={"d-none d-lg-flex mb-2 ps-2 pe-0"}>
            <div className={"w-lg-25 ps-1 pe-2"}>
                Label
            </div>
            <div className={"w-lg-10 px-2"}>
                Type
            </div>
            <div className={"w-lg-10 px-2"}>
                Format
            </div>
            <div className={"flex-fill ps-1 pe-1 ps-lg-2 pe-lg- text-end"}>
                <div className={"d-inline me-4"}>
                    Order
                </div>
               <div className={"d-inline"}>
                Categorical <Icon ariaLabel={"Info"}
                       icon={"bi-info-circle"}
                       tooltip={"Categorical fields have discrete groups of information. Only string fields can be categorical."}
                />
                </div>
            </div>
        </div>
    )
};

export const SchemaHeader = memo(SchemaHeaderInner);