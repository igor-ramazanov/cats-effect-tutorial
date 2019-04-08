import java.io._
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect._
import cats.implicits._
import cats.temp.par._
import shapeless.tag.@@

import scala.util.Try

object Copy extends IOApp {
  sealed trait BytesCountTag
  sealed trait FilesCountTag
  type BytesCount = Long @@ BytesCountTag
  type FilesCount = Long @@ FilesCountTag

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def BytesCount(n: Long): BytesCount = n.asInstanceOf[BytesCount]
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def FilesCount(n: Long): FilesCount = n.asInstanceOf[FilesCount]

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  override def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- if (args.length != 2 && args.length != 3)
        IO.raiseError(new IllegalArgumentException("Need 2 or 3 args"))
      else
        IO.unit
      _ <- if (args.head == args(1))
        IO.raiseError[Unit](
          new IllegalArgumentException("Args must be different")
        )
      else IO.unit
      bufferSize = args
        .get(2)
        .flatMap(s => Try(s.toInt).toOption)
        .filter(_ > 0)
        .getOrElse(1024)
      orig = new File(args.head)
      _ <- if (!orig.exists())
        IO.raiseError(new IllegalArgumentException("Original file must exists"))
      else IO.unit
      dest = new File(args.tail.head)
      _ <- copy[IO](orig, dest, bufferSize) >>= {
        case (bytes, files) => IO(println(s"Copied $bytes bytes, $files files"))
      }
    } yield ExitCode.Success

  def copy[F[_]: Concurrent: Par: Timer](
    orig: File,
    dest: File,
    bufferSize: Int
  ): F[(BytesCount, FilesCount)] = {
    (dest.exists(), orig.isDirectory, dest.isDirectory) match {
      case (true, true, false) | (true, false, true) =>
        Sync[F].raiseError[(BytesCount, FilesCount)](
          new IllegalArgumentException("Files must have the same type")
        )
      case (_, false, _) => copyFile[F](orig, dest, bufferSize)
      case _ =>
        for {
          created <- if (!dest.exists()) mkDir(dest) else Sync[F].pure(true)
          files <- listFiles(orig)
          results <- if (!created)
            Sync[F].delay {
              println("Couldn't create directory, won't process")
              List.empty[(BytesCount, FilesCount)]
            } else {
            files.parTraverse { file =>
              copy(file, dest.toPath.resolve(file.getName).toFile, bufferSize)
            }
          }
        } yield
          results.foldLeft((0L, 0L)) {
            case ((accBytes, accFiles), (nextBytes, nextFiles)) =>
              (accBytes + nextBytes, accFiles + nextFiles)
          } match {
            case (b, f) => (BytesCount(b), FilesCount(f))
          }
    }
  }

  def listFiles[F[_]: Sync](file: File): F[List[File]] = {
    Sync[F].delay {
      file.listFiles().toList
    }
  }

  def mkDir[F[_]: Sync](file: File): F[Boolean] = Sync[F].delay(file.mkdir())

  def copyFile[F[_]: Concurrent: Timer](
    orig: File,
    dest: File,
    bufferSize: Int
  ): F[(BytesCount, FilesCount)] =
    for {
      guard <- Sync[F].delay(new AtomicBoolean(true))
      count <- inputOutputStreams(guard, orig, dest).use {
        case (in, out) =>
          transfer(in, out, bufferSize, guard)
      }
    } yield (BytesCount(count), FilesCount(1))

  def inputOutputStreams[F[_]: Sync](
    guard: AtomicBoolean,
    orig: File,
    dest: File
  ): Resource[F, (InputStream, OutputStream)] =
    for {
      inStream <- inputStream(guard, orig)
      outStream <- outputStream(guard, dest)
    } yield (inStream, outStream)

  def inputStream[F[_]: Sync](guard: AtomicBoolean,
                              orig: File): Resource[F, FileInputStream] =
    Resource.make(Sync[F].delay(new FileInputStream(orig))) { inStream =>
      Sync[F]
        .delay {
          inStream.synchronized {
            guard.set(false)
            inStream.close()
          }
        }
        .handleErrorWith(_ => ().pure[F])
    }

  def outputStream[F[_]: Sync](guard: AtomicBoolean,
                               dest: File): Resource[F, FileOutputStream] =
    Resource.make(Sync[F].delay(new FileOutputStream(dest))) { outStream =>
      Sync[F]
        .delay {
          outStream.synchronized {
            guard.set(false)
            outStream.close()
          }
        }
        .handleErrorWith(_ => ().pure[F])
    }

  def transfer[F[_]: Sync: Timer](in: InputStream,
                                  out: OutputStream,
                                  bufferSize: Int,
                                  guard: AtomicBoolean): F[Long] = {
    def loop(buffer: Array[Byte], count: Long): F[Long] =
      for {
        c <- Sync[F].delay {
          in.synchronized {
            if (guard.get()) {
              in.read(buffer, 0, buffer.length)
            } else {
              -1
            }
          }
        }
        res <- if (c == -1) {
          Sync[F].pure(count)
        } else {
          Sync[F]
            .delay {
              out.synchronized {
                if (guard.get()) {
                  out.write(buffer, 0, c)
                  out.flush()
                }
              }
            }
            .flatMap(_ => loop(buffer, count + c))
        }
      } yield res

    for {
      buffer <- Sync[F].delay(Array.ofDim[Byte](bufferSize))
      res <- loop(buffer, 0)
    } yield res
  }
}
