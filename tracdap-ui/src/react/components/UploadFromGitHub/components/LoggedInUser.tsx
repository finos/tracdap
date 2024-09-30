/**
 * A component that shows the logged-in user's info including how many more GitHub API requests they
 * can make and when this limit resets.
 *
 * @module LoggedInUser
 * @category Component
 */

import {A} from "../../A";
import {Button} from "../../Button";
import fallbackAvatar from "../../../../images/rubber_duck_avatar.png";
import {logout, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import PropTypes from "prop-types";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the LoggedInUser component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
}

export const LoggedInUser = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const user = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].user)

    return (

        <React.Fragment>
            {/*Only show the login in no successful login has occurred, and we have got the user's details*/}
            {user.status === "succeeded" && user.details != null &&

                <div className={"py-5"}>
                    <div className={"d-flex"}>

                        <img alt={user.details.name || user.details.login}
                             className={"rounded-circle"}
                            // There is an issue in GitHub Enterprise where in private mode the avatar image of an
                            // account will have a CORB error. Here we provide a fallback for the image
                            // See: https://github.com/Reviewable/Reviewable/issues/770
                             onError={(e) => e.currentTarget.src = fallbackAvatar}
                             height={80}
                             src={user.details.avatar_url}
                             width={80}
                        />

                        <div className={"ps-3"}>
                            <span className={"git git-title fs-1 fw-lighter"}>
                                {user.details.type.toLowerCase()}/
                            </span>
                            <A className={"git git-title fs-1 fw-bolder git-link"} href={user.details.html_url}>
                                {user.details.name || user.details.login}
                            </A>
                            <span className={"fs-13 d-flex text-tertiary"}>
                               <Button ariaLabel={"Log out"}
                                       className={"p-0 my-0 ms-0 me-2"}
                                       isDispatched={true}
                                       name={storeKey}
                                       onClick={logout}
                                       size={"sm"}
                                       variant={"link"}
                               >
                                [Log out]
                            </Button>
                            </span>
                        </div>
                    </div>
                </div>

            }
        </React.Fragment>
    )
};

LoggedInUser.propTypes = {

  storeKey: PropTypes.string.isRequired
};