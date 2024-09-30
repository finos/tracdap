/**
 * This sets the types for useDispatch and useSelector hooks from the redux toolkit
 * package (RTK). This is done like this so that useAppDispatch and useAppSelector
 * can be imported into components and the useDispatch and useSelector hooks will have the
 * right types set, rather than having to import RootState and AppDispatch  and setting the
 * types each time the hook is used.
 *
 * See https://redux-toolkit.js.org/tutorials/typescript
 */

import {TypedUseSelectorHook, useDispatch, useSelector} from 'react-redux'
import type {RootState, AppDispatch} from '../storeController'

// Use throughout your app instead of plain `useDispatch` and `useSelector`
export const useAppDispatch = () => useDispatch<AppDispatch>()
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector