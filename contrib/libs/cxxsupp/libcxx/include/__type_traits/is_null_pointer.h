//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef _LIBCPP___TYPE_TRAITS_IS_NULL_POINTER_H
#define _LIBCPP___TYPE_TRAITS_IS_NULL_POINTER_H

#include <__config>
#include <__cstddef/nullptr_t.h>
#include <__type_traits/integral_constant.h>
#include <__type_traits/remove_cv.h>

#if !defined(_LIBCPP_HAS_NO_PRAGMA_SYSTEM_HEADER)
#  pragma GCC system_header
#endif

_LIBCPP_BEGIN_NAMESPACE_STD

template <class _Tp>
#if __has_builtin(__remove_cv)
inline const bool __is_null_pointer_v = __is_same(__remove_cv(_Tp), nullptr_t);
#else
inline const bool __is_null_pointer_v = __is_same(__remove_cv_t<_Tp>, nullptr_t);
#endif

#if _LIBCPP_STD_VER >= 14
template <class _Tp>
struct _LIBCPP_TEMPLATE_VIS is_null_pointer : integral_constant<bool, __is_null_pointer_v<_Tp>> {};

#  if _LIBCPP_STD_VER >= 17
template <class _Tp>
inline constexpr bool is_null_pointer_v = __is_null_pointer_v<_Tp>;
#  endif
#endif // _LIBCPP_STD_VER >= 14

_LIBCPP_END_NAMESPACE_STD

#endif // _LIBCPP___TYPE_TRAITS_IS_NULL_POINTER_H
