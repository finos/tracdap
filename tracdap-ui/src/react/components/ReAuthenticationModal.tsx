/**
 * A component that shows a modal where the user can reauthenticate their session. This also handles
 * all the initial getting and storing of the JWT information in the root application store.
 *
 * @module ReAuthenticationModal
 * @category Component
 */

import {Button} from "./Button";
import {checkJwtExpiry} from "../utils/utils_general";
import Modal from "react-bootstrap/Modal";
import React, {useEffect, useMemo, useRef, useState} from "react";
import {setLogin} from "../store/applicationStore";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";

export const ReAuthenticationModal = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {login} = useAppSelector(state => state["applicationStore"])

    // This is a function that checks JWT information in the application store to see how long is left, it is flagged
    // as close to expiry if it expires in less time than the second argument in seconds
    const jwtExpiry = useMemo(() => checkJwtExpiry(login.expiryDatetime, 120), [login.expiryDatetime])

    /**
     * A hook for storing whether to show this modal.
     */
    const [show, setShow] = useState<boolean>(jwtExpiry.jwtCloseToExpiry);

    /**
     * A hook for storing how long is left until the JWT expires.
     */
    const [jwtSecondsToExpiry, setJwtSecondsToExpiry] = useState(jwtExpiry.jwtSecondsToExpiry);

    // When the popup will appear. You look at the expiry of the token and take the number of seconds before that
    // event that you want the popup to appear. 
    const timeout = !jwtExpiry.jwtSecondsToExpiry ? null : Math.max(1000 * (jwtExpiry.jwtSecondsToExpiry - 120), 1)

    /**
     * A custom hook that sets either an interval or a timeout function that can be dynamically
     * set by changing the dependencies to the hook.
     * See https://overreacted.io/making-setinterval-declarative-with-react-hooks/
     * @param callback {function} The function to run when the interval or timeout is up.
     * @param delay {number|null} The delay for the interval or timeout to run.
     * @param type - Either 'timeout' or 'interval.
     * @param expiryDatetime - The expiry of the JWT authentication, this is
     * needed so that the useEffect runs when it changes.
     */
    function useInterval(callback: () => void, delay: null | number, type: "timeout" | "interval", expiryDatetime: null | string = null) {

        const savedCallback = useRef(() => {
        });

        // Remember the latest callback.
        useEffect(() => {
            savedCallback.current = callback;
        }, [callback]);

        // Set up the interval.
        useEffect(() => {

            function tick() {
                savedCallback.current();
            }

            if (delay !== null) {
                if (type === "interval") {

                    let id = setInterval(tick, delay);
                    return () => clearInterval(id);

                } else if (type === "timeout" && expiryDatetime) {

                    let id = setTimeout(tick, delay);
                    return () => clearTimeout(id);
                }
            }

        }, [delay, type, expiryDatetime]);
    }

    /**
     * Set up a timeout function that runs in timeout milliseconds.This
     * will show this modal. It is reset when jwtSecondsToExpiry resets.
     */
    useInterval(() => {

        console.log("LOG :: Running JWT timeout function")
        const jwtExpiry = checkJwtExpiry(login.expiryDatetime, 120)
        setJwtSecondsToExpiry(jwtExpiry.jwtSecondsToExpiry)
        setShow(true)

    }, timeout, "timeout", login.expiryDatetime)

    /**
     * Set up an interval function that runs in 1 second intervals. This is only run while the
     * modal is showing. It is reset when show resets.
     **/
    useInterval(() => {

        console.log("LOG :: Running JWT interval function")
        const jwtExpiry = checkJwtExpiry(login.expiryDatetime, 120)
        // Force a rerender
        setJwtSecondsToExpiry(jwtExpiry.jwtSecondsToExpiry)

    }, (show && jwtSecondsToExpiry && jwtSecondsToExpiry >= 0) ? 1000 : null, "interval")

    /**
     * A function that runs when the user clicks on the reauthenticate button. It sets
     * a cookie that expires in the far future for the user's domain. After that completes
     * (it's in a promise it opens a new window that closes after a short period of time)
     * that with the cookie will force the browser to reauthenticate. After this window
     * closes there is a delay to allow for the close to complete before the function to
     * get the new JWT information runs and saves the new details.
     */
    const reauthenticate = () => {

        // Refresh the store
        dispatch(setLogin())

        // Close the modal
        setShow(false)
    }

    return (

        <Modal size={"sm"}
               show={show}
               centered={true}
               backdrop={"static"}
        >
            <Modal.Header closeButton={false}>

                <Modal.Title>
                    Session expiring
                </Modal.Title>

            </Modal.Header>


            <Modal.Body>
                {jwtSecondsToExpiry && jwtSecondsToExpiry > 0 ? `Your session will expire in ${jwtSecondsToExpiry} seconds. If your session expires you will need to reauthenticate with TRAC.` : "Your session has expired."}
            </Modal.Body>
            
            <Modal.Footer>
                <Button ariaLabel={"Continue with session"}
                        onClick={reauthenticate}
                        isDispatched={false}
                        variant={"info"}
                >
                    Continue with session
                </Button>
            </Modal.Footer>
        </Modal>
    )
};