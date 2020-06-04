#
#
#            Nim's Runtime Library
#        (c) Copyright 2015 Dominik Picheta
#
#    See the file "copying.txt", included in this
#    distribution, for details about the copyright.
#

## AsyncMacro
## *************
## `asyncdispatch` module depends on the `asyncmacro` module to work properly.

import macros, strutils

proc skipUntilStmtList(node: NimNode): NimNode {.compileTime.} =
  # Skips a nest of StmtList's.
  result = node
  if node[0].kind == nnkStmtList:
    result = skipUntilStmtList(node[0])

# proc skipStmtList(node: NimNode): NimNode {.compileTime.} =
#   result = node
#   if node[0].kind == nnkStmtList:
#     result = node[0]

template createCb(retFutureSym, iteratorNameSym,
                  strName, identName, futureVarCompletions: untyped) =
  bind finished

  var nameIterVar = iteratorNameSym
  {.push stackTrace: off.}
  proc identName(udata: pointer = nil) {.closure, gcsafe.} =
    try:
      if not(nameIterVar.finished()):
        var next = nameIterVar()
        # Continue while the yielded future is already finished.
        while (not next.isNil()) and next.finished():
          next = nameIterVar()
          if nameIterVar.finished():
            break

        if next == nil:
          if not(retFutureSym.finished()):
            let msg = "Async procedure ($1) yielded `nil`, " &
                      "are you await'ing a `nil` Future?"
            raise newException(AssertionError, msg % strName)
        else:
          {.gcsafe.}:
            next.addCallback(identName)
    except CancelledError:
      retFutureSym.cancel()
    except Exception as exc:
      futureVarCompletions

      if retFutureSym.finished():
        # Take a look at tasyncexceptions for the bug which this fixes.
        # That test explains it better than I can here.
        raise exc
      else:
        retFutureSym.fail(exc)

  identName()
  {.pop.}

proc createFutureVarCompletions(futureVarIdents: seq[NimNode],
    fromNode: NimNode): NimNode {.compileTime.} =
  result = newNimNode(nnkStmtList, fromNode)
  # Add calls to complete each FutureVar parameter.
  for ident in futureVarIdents:
    # Only complete them if they have not been completed already by the user.
    # TODO: Once https://github.com/nim-lang/Nim/issues/5617 is fixed.
    # TODO: Add line info to the complete() call!
    # In the meantime, this was really useful for debugging :)
    #result.add(newCall(newIdentNode("echo"), newStrLitNode(fromNode.lineinfo)))
    result.add newIfStmt(
      (
        newCall(newIdentNode("not"),
                newDotExpr(ident, newIdentNode("finished"))),
        newCall(newIdentNode("complete"), ident)
      )
    )

proc processBody(node, retFutureSym: NimNode,
                 subTypeIsVoid: bool,
                 futureVarIdents: seq[NimNode]): NimNode {.compileTime.} =
  #echo(node.treeRepr)
  result = node
  case node.kind
  of nnkReturnStmt:
    result = newNimNode(nnkStmtList, node)

    # As I've painfully found out, the order here really DOES matter.
    result.add createFutureVarCompletions(futureVarIdents, node)

    if node[0].kind == nnkEmpty:
      if not subTypeIsVoid:
        result.add newCall(newIdentNode("complete"), retFutureSym,
            newIdentNode("result"))
      else:
        result.add newCall(newIdentNode("complete"), retFutureSym)
    else:
      let x = node[0].processBody(retFutureSym, subTypeIsVoid,
                                  futureVarIdents)
      if x.kind == nnkYieldStmt: result.add x
      else:
        result.add newCall(newIdentNode("complete"), retFutureSym, x)

    result.add newNimNode(nnkReturnStmt, node).add(newNilLit())
    return # Don't process the children of this return stmt
  of RoutineNodes-{nnkTemplateDef}:
    # skip all the nested procedure definitions
    return node
  else: discard

  for i in 0 ..< result.len:
    # We must not transform nested procedures of any form, otherwise
    # `retFutureSym` will be used for all nested procedures as their own
    # `retFuture`.
    result[i] = processBody(result[i], retFutureSym, subTypeIsVoid,
                            futureVarIdents)

proc getName(node: NimNode): string {.compileTime.} =
  case node.kind
  of nnkSym:
    return $node
  of nnkPostfix:
    return node[1].strVal
  of nnkIdent:
    return node.strVal
  of nnkEmpty:
    return "anonymous"
  else:
    error("Unknown name.")

proc getFutureVarIdents(params: NimNode): seq[NimNode] {.compileTime.} =
  result = @[]
  for i in 1 ..< len(params):
    expectKind(params[i], nnkIdentDefs)
    if params[i][1].kind == nnkBracketExpr and
       params[i][1][0].eqIdent("futurevar"):
      result.add(params[i][0])

proc isInvalidReturnType(typeName: string): bool =
  return typeName notin ["Future"] #, "FutureStream"]

proc verifyReturnType(typeName: string) {.compileTime.} =
  if typeName.isInvalidReturnType:
    error("Expected return type of 'Future' got '$1'" %
          typeName)

macro unsupported(s: static[string]): untyped =
  error s

proc asyncSingleProc(prc: NimNode): NimNode {.compileTime.} =
  ## This macro transforms a single procedure into a closure iterator.
  ## The ``async`` macro supports a stmtList holding multiple async procedures.
  if prc.kind notin {nnkProcDef, nnkLambda, nnkMethodDef, nnkDo}:
      error("Cannot transform this node kind into an async proc." &
            " proc/method definition or lambda node expected.")

  let prcName = prc.name.getName

  let returnType = prc.params[0]
  var baseType: NimNode
  # Verify that the return type is a Future[T]
  if returnType.kind == nnkBracketExpr:
    let fut = repr(returnType[0])
    verifyReturnType(fut)
    baseType = returnType[1]
  elif returnType.kind in nnkCallKinds and returnType[0].eqIdent("[]"):
    let fut = repr(returnType[1])
    verifyReturnType(fut)
    baseType = returnType[2]
  elif returnType.kind == nnkEmpty:
    baseType = returnType
  else:
    verifyReturnType(repr(returnType))

  let subtypeIsVoid = returnType.kind == nnkEmpty or
        (baseType.kind == nnkIdent and returnType[1].eqIdent("void"))

  let futureVarIdents = getFutureVarIdents(prc.params)

  var outerProcBody = newNimNode(nnkStmtList, prc.body)

  # -> var retFuture = newFuture[T]()
  var retFutureSym = ident "chronosInternalRetFuture"
  var subRetType =
    if returnType.kind == nnkEmpty: newIdentNode("void")
    else: baseType
  outerProcBody.add quote do:
    var `retFutureSym` = newFuture[`subRetType`](`prcName`)

  # -> iterator nameIter(): FutureBase {.closure.} =
  # ->   {.push warning[resultshadowed]: off.}
  # ->   var result: T
  # ->   {.pop.}
  # ->   <proc_body>
  # ->   complete(retFuture, result)
  var iteratorNameSym = genSym(nskIterator, $prcName)
  var procBody = prc.body.processBody(retFutureSym, subtypeIsVoid,
                                    futureVarIdents)
  # don't do anything with forward bodies (empty)
  if procBody.kind != nnkEmpty:
    procBody.add(createFutureVarCompletions(futureVarIdents, nil))

    if not subtypeIsVoid:
      procBody.insert(0, newNimNode(nnkPragma).add(newIdentNode("push"),
        newNimNode(nnkExprColonExpr).add(newNimNode(nnkBracketExpr).add(
          newIdentNode("warning"), newIdentNode("resultshadowed")),
        newIdentNode("off")))) # -> {.push warning[resultshadowed]: off.}

      procBody.insert(1, newNimNode(nnkVarSection, prc.body).add(
        newIdentDefs(newIdentNode("result"), baseType))) # -> var result: T

      procBody.insert(2, newNimNode(nnkPragma).add(
        newIdentNode("pop"))) # -> {.pop.})

      procBody.add(
        newCall(newIdentNode("complete"),
          retFutureSym, newIdentNode("result"))) # -> complete(retFuture, result)
    else:
      # -> complete(retFuture)
      procBody.add(newCall(newIdentNode("complete"), retFutureSym))

    var closureIterator = newProc(iteratorNameSym, [newIdentNode("FutureBase")],
                                  procBody, nnkIteratorDef)
    closureIterator.pragma = newNimNode(nnkPragma, lineInfoFrom=prc.body)
    closureIterator.addPragma(newIdentNode("closure"))

    # If proc has an explicit gcsafe pragma, we add it to iterator as well.
    if prc.pragma.findChild(it.kind in {nnkSym, nnkIdent} and $it == "gcsafe") != nil:
      closureIterator.addPragma(newIdentNode("gcsafe"))
    outerProcBody.add(closureIterator)

    # -> createCb(retFuture)
    # NOTE: The "_continue" suffix is checked for in asyncfutures.nim to produce
    # friendlier stack traces:
    var cbName = genSym(nskProc, prcName & "_continue")
    var procCb = getAst createCb(retFutureSym, iteratorNameSym,
                         newStrLitNode(prcName),
                         cbName,
                         createFutureVarCompletions(futureVarIdents, nil))
    outerProcBody.add procCb

    # -> return retFuture
    outerProcBody.add newNimNode(nnkReturnStmt, prc.body[^1]).add(retFutureSym)

  if prc.kind != nnkLambda: # TODO: Nim bug?
    prc.addPragma(newColonExpr(ident "stackTrace", ident "off"))

  result = prc

  if subtypeIsVoid:
    # Add discardable pragma.
    if returnType.kind == nnkEmpty:
      # Add Future[void]
      result.params[0] = parseExpr("Future[void]")
  if procBody.kind != nnkEmpty:
    result.body = outerProcBody
  #echo(treeRepr(result))
  #if prcName == "recvLineInto":
  #  echo(toStrLit(result))

template await*[T](f: Future[T]): auto =
  when declared(chronosInternalRetFuture):
    when not declaredInScope(chronosInternalTmpFuture):
      var chronosInternalTmpFuture {.inject.}: FutureBase
    chronosInternalTmpFuture = f
    chronosInternalRetFuture.child = chronosInternalTmpFuture
    yield chronosInternalTmpFuture
    chronosInternalTmpFuture.internalCheckComplete()
    chronosInternalRetFuture.child = nil
    cast[type(f)](chronosInternalTmpFuture).internalRead()
  else:
    unsupported "await is only available within {.async.}"

template awaitne*[T](f: Future[T]): Future[T] =
  when declared(chronosInternalRetFuture):
    when not declaredInScope(chronosInternalTmpFuture):
      var chronosInternalTmpFuture {.inject.}: FutureBase
    chronosInternalTmpFuture = f
    chronosInternalRetFuture.child = chronosInternalTmpFuture
    yield chronosInternalTmpFuture
    chronosInternalRetFuture.child = nil
    cast[type(f)](chronosInternalTmpFuture)
  else:
    unsupported "awaitne is only available within {.async.}"

macro async*(prc: untyped): untyped =
  ## Macro which processes async procedures into the appropriate
  ## iterators and yield statements.
  if prc.kind == nnkStmtList:
    for oneProc in prc:
      result = newStmtList()
      result.add asyncSingleProc(oneProc)
  else:
    result = asyncSingleProc(prc)
  when defined(nimDumpAsync):
    echo repr result
