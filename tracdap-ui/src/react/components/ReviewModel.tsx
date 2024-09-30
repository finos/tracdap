/**
 * A component that shows summary information about the selected model and then a button to allow
 * the user to open a modal with the full information. This is used by both the {@link ObjectSummaryScene}
 * and the {@link RunAModelScene} scenes.
 *
 * @module ReviewModel
 * @category Component
 */

import {Button} from "./Button";
import {HeaderTitle} from "./HeaderTitle";
import {ModelInfoModal} from "./ModelInfoModal";
import {ObjectDetails} from "./ObjectDetails";
import PropTypes from "prop-types";
import React, {useCallback, useState} from "react";
import {ShowHideDetails} from "./ShowHideDetails";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the ReviewJob component.
 */
export interface Props {

    /**
     * The metadata tag for the model being reviewed.
     */
    tag?: trac.metadata.ITag
}

export const ReviewModel = (props: Props) => {

    const {tag} = props

    // Whether to show the modal containing info about the flow.
    const [show, setShow] = useState<boolean>(false)

    /**
     * A function that toggles showing the info modal for the user selected model.
     */
    const toggleInfoModal = useCallback(() => {
        setShow(show => !show)
    }, [])

    return (
        <React.Fragment>
            {tag &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Model summary"}>
                        <Button ariaLabel={"Show model info"}
                                isDispatched={false}
                                onClick={toggleInfoModal}
                                variant={"outline-info"}
                        >
                            View full info
                        </Button>
                    </HeaderTitle>

                    <ShowHideDetails linkText={"model details"}
                                     classNameInner={"pt-4pb-0"}
                                     classNameOuter={"mt-0 pb-4"}
                                     showOnOpen={false}>
                        <ObjectDetails metadata={tag} bordered={false} striped={true}/>
                    </ShowHideDetails>

                    <ModelInfoModal show={show}
                                    toggle={toggleInfoModal}
                                    tagSelector={tag.header}
                    />
                </React.Fragment>

            }
        </React.Fragment>
    )
};

ReviewModel.propTypes = {

    tag: PropTypes.object
};