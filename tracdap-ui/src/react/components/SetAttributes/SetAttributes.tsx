/**
 * A component that shows a list of attributes/parameters that can be set for a given TRAC object type and allows
 * the user to edit them. Some pre-existing attributes can not be modified because they are internal to the application.
 *
 * @remarks Note that the {@link OnLoad} component initiates a request to TRAC to get datasets for the UI that are then
 * stored in the {@link applicationSetupStore} store. One of these datasets is the list of attributes available. The
 * {@link applicationSetupStore} also processes these attributes into a format that can be used by the UI and then updates
 * the {@link setAttributesStore} so that all the use cases are populated and default values set without this component
 * having to be mounted.
 *
 * @module SetAttributes
 * @category Component
 */

import {HeaderTitle} from "../HeaderTitle";
import {ParameterMenu} from "../ParameterMenu/ParameterMenu";
import React from "react";
import {setAttribute, SetAttributesStoreState} from "./setAttributesStore";
import {TextBlock} from "../TextBlock";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the SetAttributes component.
 */
export interface Props {

    /**
     * Whether to show the menu of attributes.
     */
    show: boolean
    /**
     * The key in the SetAttributesStoreState to get the state for this component
     */
    storeKey: keyof SetAttributesStoreState["uses"]
    /**
     * The title to show as a header above the menu of attributes.
     */
    title?: string
}

export const SetAttributes = (props: React.PropsWithChildren<Props>) => {

    const {children, show, storeKey, title} = props

    // Get what we need from the store
    const {
        lastAttributeChanged,
        processedAttributes,
        validation: {validationChecked},
        values
    } = useAppSelector(state => state["setAttributesStore"].uses[storeKey].attributes)

    return (
        <React.Fragment>
            {show &&
                <React.Fragment>
                    {title && <HeaderTitle type={"h3"} text={title}/>}

                    {children && <TextBlock>
                        {children}
                    </TextBlock>
                    }

                    <ParameterMenu label={"attributes"}
                                   lastParameterChanged={lastAttributeChanged}
                                   storeKey={storeKey}
                                   onChange={setAttribute}
                                   parameters={processedAttributes || {}}
                                   values={values}
                                   validationChecked={validationChecked}
                    />
                </React.Fragment>
            }
        </React.Fragment>
    )
};