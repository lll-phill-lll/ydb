// written by build/gn/scripts/pch_update.py
#pragma once

#include <__chrono/hh_mm_ss.h>
#include <__chrono/literals.h>
#include <__chrono/statically_widen.h>
#include <__chrono/year_month_weekday.h>
#include <__format/format_context.h>
#include <__format/parser_std_format_spec.h>
#include <__math/hypot.h>
#include <sstream>
#include <stack>
#include "contrib/libs/llvm16/include/llvm/CodeGen/AsmPrinter.h"
#include "contrib/libs/llvm16/include/llvm/CodeGen/LiveInterval.h"
#include "contrib/libs/llvm16/include/llvm/CodeGen/MachineOutliner.h"
#include "contrib/libs/llvm16/include/llvm/IR/Argument.h"
#include "contrib/libs/llvm16/include/llvm/IR/DataLayout.h"
#include "contrib/libs/llvm16/include/llvm/IR/DebugInfoMetadata.h"
#include "contrib/libs/llvm16/include/llvm/MC/MCContext.h"
#include "contrib/libs/llvm16/include/llvm/Support/TimeProfiler.h"
