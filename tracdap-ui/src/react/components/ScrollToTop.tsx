/**
 * A component that is loaded once in the application and that runs a hook when the URL of the application changes.
 * This causes the window to scroll to the top of the page.
 * See {@link https://stackoverflow.com/questions/65500832/react-router-v6-window-scrollto-does-not-work| this guide}.
 *
 * @module ScrollToTop
 * @category Component
 */

import {goToTop} from "../utils/utils_general";
import React, {useEffect} from "react";
import {useLocation} from 'react-router';

export const ScrollToTop = () => {

    const {pathname} = useLocation()

    useEffect(() => {

        goToTop()

    }, [pathname])

    return null
}