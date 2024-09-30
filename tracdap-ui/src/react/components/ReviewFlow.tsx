/**
 * A component that shows summary information about the selected flow and then a button to allow
 * the user to open a modal with the full information. This is used by both the {@link ObjectSummaryScene}
 * and the {@link RunAFlowScene} scenes.
 *
 * @module ReviewFlow
 * @category Component
 */

import {Button} from "./Button";
import {FlowInfoModal} from "./FlowInfoModal";
import {HeaderTitle} from "./HeaderTitle";
import {ObjectDetails} from "./ObjectDetails";
import PropTypes from "prop-types";
import React, {useCallback, useState} from "react";
import {ShowHideDetails} from "./ShowHideDetails";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {ObjectSummaryStoreState} from "../scenes/ObjectSummaryScene/store/objectSummaryStore";

/**
 * An interface for the props of the ReviewFlow component.
 */
export interface Props {
        /**
     * The key in the objectSummaryStore to get the state for this component
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
    /**
     * The TRAC metadata for the flow shown the information for.
     */
    tag: trac.metadata.ITag | undefined
}

export const ReviewFlow = (props: Props) => {

    const {storeKey, tag} = props

    // Whether to show the modal containing info about the flow.
    const [show, setShow] = useState<boolean>(false)

    /**
     * A function that toggles showing the info modal for the user selected flow.
     */
    const toggleInfoModal = useCallback(() => {
        setShow(show => !show)
    }, [])

    return (
        <React.Fragment>
            {tag &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Flow summary"}>
                        <Button ariaLabel={"Show flow info"}
                                isDispatched={false}
                                onClick={toggleInfoModal}
                                variant={"outline-info"}
                        >
                            View full info
                        </Button>
                    </HeaderTitle>

                    <ShowHideDetails linkText={"flow details"}
                                     classNameInner={"pt-4pb-0"}
                                     classNameOuter={"mt-0 pb-4"}
                                     showOnOpen={false}>
                        <ObjectDetails metadata={tag} bordered={false} striped={true}/>
                    </ShowHideDetails>

                    <FlowInfoModal show={show}
                                   storeKey={storeKey}
                                   toggle={toggleInfoModal}
                                   tagSelector={tag.header}
                    />
                </React.Fragment>

            }
        </React.Fragment>
    )
};

ReviewFlow.propTypes = {

    storeKey: PropTypes.string.isRequired,
    tag: PropTypes.object.isRequired
};
